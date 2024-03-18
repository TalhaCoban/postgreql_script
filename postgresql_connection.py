import psycopg2
import arcpy
import json
import os
from datatypes import extra_tables, data_type_mappings as dtypes, data_type_mappings_esri as dtype_es



ARCGIS_PROJECT_FOLDER = "INTEGRATION"
MAINPATH = os.path.join(os.getcwd(), ARCGIS_PROJECT_FOLDER)
if not os.path.exists(MAINPATH):
    os.mkdir(MAINPATH)



class Connect():
    
    def __init__(self, DB, extra_tables, data_type_mappings, data_type_mappings_esri, tables=None, schema="public", ProjectFile=None):
        
        self.DB = DB
        self.extra_tables = extra_tables
        self.data_type_mappings = data_type_mappings
        self.data_type_mappings_esri = data_type_mappings_esri
        self.tables = tables
        self.schema = schema
        self.ProjectFile = ProjectFile
        self.add_database_file_using_arcpy()
        self.Connect_DB()


    # Bu classın bir örneği alındığında veritbanına bağlanır ve bizden komut bekler.
    def Connect_DB(self):

        self.conn = psycopg2.connect(
            database = self.DB["database"],
            user = self.DB["user"],
            password = self.DB["password"],
            host = self.DB["host"],
            port = self.DB["port"]
        )
        self.cursor = self.conn.cursor()

    
    # verilen tablo listesini veritabanında arar ve bulabildiği tüm bilgileri çeker geriye json döndürür.
    # Eğer tablo listesi başta belirtilmemişsse ne kadar tablo varsa hepsini bilgileri ile birlikte getirir.
    def get_table_column_names(self) -> (dict, list):

        messages = []
        
        tables = self.tables
        if tables == None:
            self.cursor.execute(f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{self.schema}';")
            rows = self.cursor.fetchall()
            tables = [row[0] for row in rows]
            datatype_control = False
        else:
            datatype_control = True
            
        table_column_names = dict()
        for table_name in tables:
            inner_dict = dict()
            column_names = []
            if table_name in self.extra_tables:
                continue

            query = f"""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = '{table_name}'
                ORDER BY ordinal_position;
            """
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
            for row in rows:
                if row[1] == "USER-DEFINED":
                    try:
                        query = f"""
                            SELECT f_geometry_column, type, srid
                            FROM geometry_columns
                            WHERE f_table_name = '{table_name}' AND f_table_schema = '{self.schema}';
                        """
                        self.cursor.execute(query)
                        geom = self.cursor.fetchall()
                        cont = True

                    except psycopg2.errors.UndefinedTable:
                        self.Close_DB()
                        self.Connect_DB()
                        geom = set()
                        cont = True
                    
                    except psycopg2.errors.InvalidTextRepresentation:
                        self.Close_DB()
                        self.Connect_DB()
                        geom = set()
                        cont = False
                        self.extra_tables.append(table_name)

                    if len(geom) == 0 and cont:
                        query = f"""
                            SELECT column_name, srid
                            FROM st_geometry_columns
                            WHERE table_name = %s and schema_name = '{self.schema}';
                        """
                        self.cursor.execute(query, (table_name,))
                        geom = self.cursor.fetchone()
                        if geom[0] == "shape":                            
                            self.cursor.execute(f"select geometry_type('{self.schema}', '{table_name}', '{geom[0]}');")
                            geom_type =  self.cursor.fetchone()[0]
                            inner_dict["geom_column"] = (geom[0], geom_type)
                            inner_dict["SRID"] = geom[1]
                            inner_dict["owner"] = "esri"
                        else:
                            self.extra_tables.append(table_name)
                            print(f"'{table_name}' tablosu geometrik kolunu bulunamadığı için işleneme alınmayacak")
                            messages.append(("common_problems",f"'{table_name}' tablosu geometrik kolunu bulunamadığı için işleneme alınmayacak"))
                    elif cont:
                        geom = geom[0]
                        if geom[1] in list(self.data_type_mappings["geometry_types"].keys()):
                            geomtype = self.data_type_mappings["geometry_types"][geom[1]]
                            if geomtype == "GEOMETRY" and datatype_control:
                                query = f'select distinct GeometryType("{geom[0]}") from "{table_name}"'
                                self.cursor.execute(query)
                                geomtypes = self.cursor.fetchall()
                                if len(geomtypes) == 1:
                                    geomtype = geomtypes[0][0]
                                elif len(geomtypes) == 2:
                                    if geomtypes[0][0] != None and geomtypes[1][0] != None:
                                        if geomtypes[0][0].lower().replace("multi", "").strip() == geomtypes[1][0].lower().replace("multi", "").strip():
                                            geomtype = geomtypes[0][0].lower().replace("multi", "").strip()
                                    else:
                                        geomtype = "GEOMETRY"
                                else:
                                    geomtype = "GEOMETRY"
                            inner_dict["geom_column"] = (geom[0], geomtype)
                            inner_dict["SRID"] = geom[2]
                            inner_dict["owner"] = "postgis"
                        else:
                            self.extra_tables.append(table_name)
                            print(f"'{table_name}' tablosu geometrik kolunu komplex veri tipinde olduğu için işleneme alınmayacak")
                            messages.append(("common_problems",f"'{table_name}' tablosu geometrik kolunu komplex '{geom[1]}' veri tipinde olduğu için işleneme alınmayacak"))
                else:
                    if row[1] in list(self.data_type_mappings.keys()):
                        column_names.append((row[0], self.data_type_mappings[row[1]]))
                    else:
                        self.extra_tables.append(table_name)
                        print(f"'{table_name}' tablosu komplex veri tipinde '{row[0]}' kolon olduğu için işleneme alınmayacak")
                        messages.append(("common_problems",f"'{table_name}' tablosu komplex veri tipinde '{row[0]}' kolon olduğu için işleneme alınmayacak"))
            
            inner_dict["columns"] = column_names
            if "geom_column" not in inner_dict.keys():
                inner_dict["geom_column"] = None
            if "SRID" not in inner_dict.keys():
                inner_dict["SRID"] = None
            if "owner" not in inner_dict.keys():
                inner_dict["owner"] = None

            query = f"""
                SELECT column_name
                FROM information_schema.key_column_usage
                WHERE table_name = '{table_name}';
            """
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
            if len(rows) == 0:
                if "objectid" in [ i[0] for i in column_names ]:
                    inner_dict["primekey"] = "objectid"
                else:
                    self.extra_tables.append(table_name)
                    print(f"'{table_name}' tablosunda primary key kolunu bulunmadığı için işleneme alınmayacak")
                    messages.append(("common_problems",f"'{table_name}' tablosunda primary key kolunu bulunmadığı için işleneme alınmayacak"))
                    continue
            else:
                for row in rows:
                    inner_dict["primekey"] = row[0]

            table_column_names[table_name] = inner_dict
        """
        with open("tables.json", "w", encoding="utf-8") as file:
            json.dump(table_column_names, file)
        """
        return table_column_names, messages


    # burada herhangibir tablonun objectid 'sinin en küçük ve em büyük değerlerini alıyorum. Bu bilgiyi arcpy ile tablo oluşturunca sequence ayarlamak için kullanıyorum.
    def select_min_max_primekey(self, tablename, primekey) -> tuple:

        query = f'Select min("{primekey}"), max("{primekey}"), count("{primekey}") from "{tablename}";'
        self.cursor.execute(query)
        rows = self.cursor.fetchone()
        return rows


    # verilen parametrelere göre select işlemi yapar. eğer geometerik veri kolonu varsa onu wkt olarak alır ve en sona atar.
    def Select_Values(self, table:str, column_names, geom_column, fetchall:bool, where="") -> (list, bool):

        try:
            joined_columns = '", "'.join(column_names)
            query = f'SELECT "{joined_columns}"'
            
            geom_returned = False
            if geom_column != None:
                query += f', st_astext("{geom_column}")'
                geom_returned = True

            if where == "":
                query += f' FROM "{table}";'
            else:
                query += f' FROM "{table}" WHERE {where};'

            self.cursor.execute(query)

            if fetchall:
                rows = self.cursor.fetchall()
            else:
                rows = self.cursor.fetchone()

            return rows, geom_returned
    
        except Exception as error:
            self.Close_DB()
            self.Connect_DB()
            return None, False


    # insert işlemi yapar. bunu yaparken ilk önce sadece id değeri ile bir insert yapar. Daha sonra diğer alanları teker teker günceller. 
    # columns_values bir sözlük yapısı. anahtar değeri kolon isimi değer ise değer şeklinde bir satırlık veri oluyor
    # ID ise primary key kolomunun ismi. Bu kolumun aynı zamanda columns_values içinde olması gerekiyor
    # geom_column_target geomegerik verinin bulunduğpu kolum ismi. Aynı şekilde Bu kolumun da aynı zamanda columns_values içinde olması gerekiyor. Geometrik değer update ile güncellniyor.
    # owner = public veya sde olacak. Bu verinin postgis veya esri tarafında yönetildiğini gösterecek.
    def Insert_Value(self, table_name:str, columns_values:dict, ID:str, geom_column_target, SRID, owner, nextval) -> bool:

        try:
            id = columns_values[ID]
            if nextval:
                getid_query = f'select max(objectid) from "{table_name}";'
                self.cursor.execute(getid_query)
                objectid = self.cursor.fetchone()[0]
                if objectid == None:
                    objectid = 1
                else: 
                    objectid = int(objectid) + 1
                query = f'INSERT INTO "{table_name}" (objectid, {ID}) VALUES ({objectid}, {id});'
            else:
                query = f'INSERT INTO "{table_name}" ("{ID}") VALUES ({id});'

            self.cursor.execute(query)
            self.conn.commit()

            columns_values.pop(ID)

            for column in columns_values.keys():
                value = columns_values[column]
                success = self.Update_value(
                    table_name, 
                    column, 
                    value, 
                    ID, 
                    id,
                    geom_column_target,
                    SRID, 
                    owner
                )
            return True

        except Exception as error:
            self.Close_DB()
            self.Connect_DB()
            return False


    # burası update fonksiyonu. Insert işlemlerinin devamı burddan yürüyor. Bu programı yavaşlatabilir ama her hangibir hata durumunda sadece o değer girilmemiş olur. bütük bir satır değil.
    # Tek bir kolon ismi ve değeri şeklinde ilerliyor. geom_column_target verilirse ve column değerine eşit olursa geometeri uğdate ediyor. Burası tuhaf görünebilir ama bu classın diğer fonksiyonları ile kullanıldığında yararlı oluyor.
    # owner = public veya sde olacak. Bu verinin postgis veya esri tarafında yönetildiğini gösterecek.
    def Update_value(self, table_name:str, column:str, new_value, ID:str, id, geom_column_target=None, SRID=None, owner="public") -> bool:

        try:
            if (column == geom_column_target):
                if new_value != None:
                    if owner == "esri":
                        cont = True
                        new_value = new_value.lower()
                        if new_value.startswith("point"):
                            query = f"update {table_name} set {geom_column_target} = st_point('{new_value}', {SRID}) WHERE {ID} = '{id}';"
                        elif new_value.startswith("linestring"):
                            query = f"update {table_name} set {geom_column_target} = st_linestring('{new_value}', {SRID}) WHERE {ID} = '{id}';"
                        elif new_value.startswith("polygon"):
                            query = f"update {table_name} set {geom_column_target} = st_polygon('{new_value}', {SRID}) WHERE {ID} = '{id}';"
                        elif new_value.startswith("multipoint"):
                            query = f"update {table_name} set {geom_column_target} = st_multipoint('{new_value}', {SRID}) WHERE {ID} = '{id}';"
                        elif new_value.startswith("multilinestring"):
                            query = f"update {table_name} set {geom_column_target} = st_multilinestring('{new_value}', {SRID}) WHERE {ID} = '{id}';"
                        elif new_value.startswith("multipolygon"):
                            query = f"update {table_name} set {geom_column_target} = st_multipolygon('{new_value}', {SRID}) WHERE {ID} = '{id}';"
                        else:
                            print(f"{ID} = {id} olan veri\n{new_value}\n point, polyline veya polygon olmadığı işlenemiyor")
                            cont = False
                        if cont:
                            try:
                                self.cursor.execute(query)  
                                print(f"{ID} = {id} olan verinin '{column}' değeri '{new_value}' olarak güncellendi")
                            except psycopg2.errors.InFailedSqlTransaction as error:
                                self.Close_DB()
                                self.Connect_DB()
                                print(f"{ID} = {id} olan veri\n{new_value}\n {error} hatası sebebiyle işlenemiyor\n")
                    elif owner == "postgis":
                        try:
                            query = f'UPDATE "{table_name}" SET "{geom_column_target}" = ST_GeomFromText(%s) WHERE "{ID}"=%s;'
                            self.cursor.execute(query, (new_value, id)) 
                            print(f"{ID} = {id} olan verinin '{column}' değeri '{new_value}' olarak güncellendi")
                        except psycopg2.errors.InvalidParameterValue:
                            self.Close_DB()
                            self.Connect_DB()
                            query = f'UPDATE "{table_name}" SET "{geom_column_target}" = ST_GeomFromText(ST_AsText(ST_Multi(%s))) WHERE "{ID}"=%s;'
                            self.cursor.execute(query, (new_value, id)) 
                            print(f"{ID} = {id} olan verinin '{column}' değeri '{new_value}' olarak güncellendi")
                    else:
                        query = f'UPDATE "{table_name}" SET "{geom_column_target}" = ST_GeomFromText(%s) WHERE "{ID}"=%s;'
                        self.cursor.execute(query, (new_value, id))  
                        print(f"{ID} = {id} olan verinin '{column}' değeri '{new_value}' olarak güncellendi")
                else:
                    query = f'UPDATE "{table_name}" SET "{geom_column_target}" = %s WHERE "{ID}"=%s;'
                    self.cursor.execute(query, (new_value, id))
                    print(f"{ID} = {id} olan verinin '{column}' değeri '{new_value}' olarak güncellendi")
            else:
                query = f'UPDATE "{table_name}" SET "{column}" = %s WHERE "{ID}"=%s;'
                self.cursor.execute(query, (new_value, id))
                print(f"{ID} = {id} olan verinin '{column}' değeri '{new_value}' olarak güncellendi")

            self.conn.commit()
            return True

        except psycopg2.errors.UndefinedColumn:
            print(f"'{column}' kolonu '{table_name}' tablosunda bulunmuyor")
            self.Close_DB()
            self.Connect_DB()
            return False
        
        except psycopg2.errors.StringDataRightTruncation:
            print(f"'işlem gerçekleşmedi.\n{column} kolonu için çok büyük deger:\n '{new_value}'")
            self.Close_DB()
            self.Connect_DB()
            return False
        
        except Exception as error:
            print(f"{ID} = {id} olan verinin {column} değeri {new_value} olarak güncellenemedi. Sebebi;\n {error}")
            self.Close_DB()
            self.Connect_DB()
            return False

    # Bir satırlık değer silmek için 
    def Delete_Row(self, table_name:str, id_column, id) -> bool:
        
        query = f'DELETE FROM "{table_name}" WHERE "{id_column}"' + f"='{id}';"
        self.cursor.execute(query)
        self.conn.commit()
        print(f"{id_column} = {id} olan veri '{table_name}' tablosundan silindi")       
        return True

    # 2 tane geometri verip eşit olup olmadğını burdan kontrol edebiliriz.
    def check_geometries(self, table_name, geom1:str, geom2:str, owner:str) -> bool:

        try:
            if owner == "esri":
                if geom1 != None and geom2 != None:
                    if geom1.lower().startswith("point") and geom2.lower().startswith("point"):
                        query = f"select st_equals(st_point('{geom1}'), st_point('{geom2}'));"
                    elif geom1.lower().startswith("point") and geom2.lower().startswith("multipoint"):
                        query = f"select st_equals(st_point('{geom1}'), st_multipoint('{geom2}'));"
                    elif geom1.lower().startswith("multipoint") and geom2.lower().startswith("point"):
                        query = f"select st_equals(st_multipoint('{geom1}'), st_point('{geom2}'));"
                    elif geom1.lower().startswith("multipoint") and geom2.lower().startswith("multipoint"):
                        query = f"select st_equals(st_multipoint('{geom1}'), st_multipoint('{geom2}'));"
                    elif geom1.lower().startswith("linestring") and geom2.lower().startswith("linestring"):
                        query = f"select st_equals(st_linestring('{geom1}'), st_linestring('{geom2}'));"
                    elif geom1.lower().startswith("linestring") and geom2.lower().startswith("multilinestring"):
                        query = f"select st_equals(st_linestring('{geom1}'), st_multilinestring('{geom2}'));"
                    elif geom1.lower().startswith("multilinestring") and geom2.lower().startswith("linestring"):
                        query = f"select st_equals(st_multilinestring('{geom1}'), st_linestring('{geom2}'));"
                    elif geom1.lower().startswith("multilinestring") and geom2.lower().startswith("multilinestring"):
                        query = f"select st_equals(st_multilinestring('{geom1}'), st_multilinestring('{geom2}'));"
                    elif geom1.lower().startswith("polygon") and geom2.lower().startswith("polygon"):
                        query = f"select st_equals(st_polygon('{geom1}'), st_polygon('{geom2}'));"
                    elif geom1.lower().startswith("polygon") and geom2.lower().startswith("multipolygon"):
                        query = f"select st_equals(st_polygon('{geom1}'), st_multipolygon('{geom2}'));"
                    elif geom1.lower().startswith("multipolygon") and geom2.lower().startswith("polygon"):
                        query = f"select st_equals(st_multipolygon('{geom1}'), st_polygon('{geom2}'));"
                    elif geom1.lower().startswith("multipolygon") and geom2.lower().startswith("multipolygon"):
                        query = f"select st_equals(st_multipolygon('{geom1}'), st_multipolygon('{geom2}'));"
                    else:
                        print(f"'{table_name}' tablosunda bilinmeyen veriler ({geom1.split('('[0])} : {geom2.split('('[0])}) olduğu için o veri işleme alınmayacak")
                        return True
                    self.cursor.execute(query)
                    answer = self.cursor.fetchone()[0]
                    if answer == True or answer == False:
                        return answer
                    else:
                        return True
                else:
                    if geom1 == geom2:
                        return True
                    else:
                        return False
                    
            elif owner == "postgis":
                if geom1 != None and geom2 != None:
                    query = f"select st_equals('{geom1}', '{geom2}');"
                    self.cursor.execute(query)
                    answer = self.cursor.fetchone()[0]
                    if answer == True or answer == False:
                        return answer
                    else:
                        return True

                else:
                    if geom1 == geom2:
                        return True
                    else:
                        return False
            
            else:
                return True

        except Exception as error:
            self.Close_DB()
            self.Connect_DB()
            return True

    # sde dosyası oluşturmak için kullanılırç Eğer class örneklernirken bir project file verllmişsse burası otomatik çalışır.
    def add_database_file_using_arcpy(self) -> bool:

        workspace = MAINPATH
        connection_name = self.ProjectFile
        if self.ProjectFile != None:
            if not os.path.exists(os.path.join(workspace, connection_name)):
                database_platform = "POSTGRESQL"
                instance = self.DB["host"] 
                database = self.DB["database"] 
                username = self.DB["user"]
                password = self.DB["password"]

                arcpy.CreateDatabaseConnection_management(
                    workspace,
                    connection_name,
                    database_platform,
                    instance,
                    "DATABASE_AUTH",
                    username,
                    password,
                    "SAVE_USERNAME",
                    database
                )
                print("Connection file created successfully: {}".format(connection_name))
        return True


    #  Burası geometrik değer barındıran bir tablonun coordinate sistemini text olarak alırç 
    def get_spatial_refence_system_srtext(self, table_name, geom_column, owner) -> str:

        self.Close_DB()
        try:
            self.conn = psycopg2.connect(
                database = self.DB["database"],
                user = "postgres",
                password = self.DB["password"],
                host = self.DB["host"],
                port = self.DB["port"]
            )
            self.cursor = self.conn.cursor()
        except psycopg2.OperationalError:
            self.Close_DB()
            self.Connect_DB()

        try:
            if owner == "postgis":
                query = f"select srtext from spatial_ref_sys where srid = (select srid from geometry_columns where f_table_name = '{table_name}');"
                self.cursor.execute(query)
                srtext = self.cursor.fetchone()
                if srtext != None:
                    srtext = srtext[0]
                else:
                    query1 = f'select St_SRID("{geom_column}") as srid, count(St_SRID("{geom_column}")) as total from "{table_name}" group by srid order by total desc limit 1'
                    self.cursor.execute(query1)
                    srid_count = self.cursor.fetchone()
                    if srid_count != None:
                        srid = srid_count[0]
                        query2 = f'select srtext from spatial_ref_sys where srid = {srid}'
                        self.cursor.execute(query2)
                        srtext = self.cursor.fetchone()
                        if srtext != None:
                            srtext = srtext[0]
                        else:
                            srtext = None
                    else:
                        srtext = None

            elif owner == "esri":
                query = f"select srtext from sde_spatial_references where srid = (select srid from st_geometry_columns where table_name = '{table_name}');"
                self.cursor.execute(query)
                srtext = self.cursor.fetchone()

                if srtext != None:
                    srtext = srtext[0]
                else:
                    query = f"select srtext from sde_spatial_references where srid = {srid}"
                    self.cursor.execute(query)
                    srtext = self.cursor.fetchone()

                    if srtext != None:
                        srtext = srtext[0]
                    else:
                        srtext = None
            
            else:
                srtext = None

        except Exception as error:
            self.extra_tables.append(table_name)
            srtext = None

        self.Close_DB()
        self.Connect_DB()
        if srtext == None:
            return """GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",SPHEROID["WGS_1984",6378137.0,298.257223563]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]]"""
        else:
            return srtext
            

    # Bu fonksiyon sql komutları ile tablo oluşturur.
    # column_names [ ("id", "character variying"), ...] şeklinde her bir kolumu isim ve veritipi şeklinde tuple ile doldurulmuş liste şeklindedir.
    # bu veritipleri bu classın get_table_column_names fonksiyonu ile alınan ve bu scripte importlanan datatypes scripte içinde görülebilecek olan tiptedir.
    def create_table(self, table_name, column_names, geom_column, primekey, SRID=None) -> list:

        column_list = list()
        for column_name in column_names:
            if column_name[0] == primekey:
                column_list.append(f'"{column_name[0]}" {column_name[1]} PRIMARY KEY')
            else:
                column_list.append(f'"{column_name[0]}" {column_name[1]}')

        if geom_column != None:
            if geom_column[1].lower().replace("multi", "") in ["point", "linestring", "polygon"]:
                if SRID != None:
                    column_list.append(f'"{geom_column[0]}" geometry({geom_column[1]}, {SRID})')
                else:
                    column_list.append(f'"{geom_column[0]}" geometry({geom_column[1]})')
            else:
                if SRID != None:
                    column_list.append(f'"{geom_column[0]}" geometry(GEOMETRY, {SRID})')
                else:
                    column_list.append(f'"{geom_column[0]}" geometry(GEOMETRY)')
        query = 'CREATE TABLE "{table_name}" ({fields});'.format(table_name = table_name, fields = '\n\t' + ",\n\t".join(column_list)+ '\n')

        try:
            self.cursor.execute(query)
            print(query, "\n")
            self.conn.commit()
            return True
        
        except psycopg2.errors.UndefinedObject as err:
            print("oluşturduğunuz veritabanına postgis kurmalısınız")
            self.extra_tables.append(table_name)
            return False

        except Exception as error:
            self.extra_tables.append(table_name)
            self.Close_DB()
            self.Connect_DB()
            return False

    # burada arcpy ile oluşturulmuş bir tabloya elle arcgis üzerinden yeni bnir veri girerken objectid sequence'i başlatmak istediüimiz sayıya diziyi getirir.
    def adjust_objectid_sequence(self, feature_class_or_table, limit):

        arcpy.env.workspace = arcpy.os.path.join(MAINPATH, self.ProjectFile)
        fields = ["objectid"]
        cursor = arcpy.da.InsertCursor(feature_class_or_table, fields)
        new_value = "MyNewValue"
        for _ in range(limit):
            cursor.insertRow([new_value])
        del cursor


    # arcpy ile feature class oluştururr. kolumları daha sonra ekliyoruz.
    def create_featureclass_using_arcpy(self, feature_class_name, geometry, srtext, minmax) -> list:
        
        try:
            workspace = arcpy.os.path.join(MAINPATH, self.ProjectFile)
            if geometry[1] == None:
                geometry_type = "GEOMETRY"
            else:
                geometry_type = self.data_type_mappings_esri["geometry_types"][geometry[1].lower()]
            spatial_reference = arcpy.SpatialReference(text=srtext) 
            if geometry_type == "GEOMETRY":
                for geomtype in ["POINT", "POLYLINE", "POLYGON"]:
                    feature_class = feature_class_name + "_" + geomtype.lower() + "_"
                    full_path = arcpy.os.path.join(workspace, feature_class)
                    if not arcpy.Exists(full_path):
                        arcpy.management.CreateFeatureclass(
                            workspace, 
                            feature_class,
                            geomtype,
                            "",
                            "DISABLED",
                            "DISABLED",
                            spatial_reference
                        )
                        print("'{}' tablosu başarıyla oluşturuldu".format(feature_class))
                        min, max, count = minmax
                        if min != None and max != None:
                            if (isinstance(min, int) or isinstance(min, float)) and (isinstance(max, int) or isinstance(max, float)):
                                if min < 10000:
                                    if max < 5000:
                                        limit = 20000
                                        self.adjust_objectid_sequence(feature_class, limit)
                                    elif max >= 5000 and max < 100000 and count > 100:
                                        limit = 200000
                                        self.adjust_objectid_sequence(feature_class, limit)
                                    elif max >= 100000 and max < 500000 and count > 500:
                                        limit = 700000
                                        self.adjust_objectid_sequence(feature_class, limit)
                                    elif max >= 500000 and max < 1100000 and count > 1000:
                                        limit = 1500000
                                        self.adjust_objectid_sequence(feature_class, limit)
                                    else:
                                        pass
                return True
            else:
                full_path = arcpy.os.path.join(workspace, feature_class_name)
                if not arcpy.Exists(full_path):
                    arcpy.management.CreateFeatureclass(
                        workspace, 
                        feature_class_name,
                        geometry_type,
                        "",
                        "DISABLED",
                        "DISABLED",
                        spatial_reference
                    )
                    print("'{}' tablosu başarıyla oluşturuldu".format(feature_class_name))
                    min, max, count = minmax
                    if min != None and max != None:
                        if (isinstance(min, int) or isinstance(min, float)) and (isinstance(max, int) or isinstance(max, float)):
                            if min < 10000:
                                if max < 5000:
                                    limit = 20000
                                    self.adjust_objectid_sequence(feature_class_name, limit)
                                elif max >= 5000 and max < 100000 and count > 100:
                                    limit = 200000
                                    self.adjust_objectid_sequence(feature_class_name, limit)
                                elif max >= 100000 and max < 500000 and count > 500:
                                    limit = 700000
                                    self.adjust_objectid_sequence(feature_class_name, limit)
                                elif max >= 500000 and max < 1100000 and count > 1000:
                                    limit = 1500000
                                    self.adjust_objectid_sequence(feature_class_name, limit)
                                else:
                                    pass
                return True
  
        except Exception as error:
            self.extra_tables.append(feature_class_name)
            return False


    # arcpy ile tablo oluştururr. kolumları daha sonra ekliyoruz.
    def create_table_using_arcpy(self, table_name, minmax) -> bool:

        try:
            workspace = os.path.join(MAINPATH, self.ProjectFile)
            arcpy.management.CreateTable(
                workspace,
                table_name
            )
            print("'{}' tablosu başarıyla oluşturuldu".format(table_name))
            min, max, count = minmax
            if min != None and max != None:
                if (isinstance(min, int) or isinstance(min, float)) and (isinstance(max, int) or isinstance(max, float)):
                    if min < 10000:
                        if max < 5000:
                            limit = 20000
                            self.adjust_objectid_sequence(table_name, limit)
                        elif max >= 5000 and max < 100000 and count > 100:
                            limit = 200000
                            self.adjust_objectid_sequence(table_name, limit)
                        elif max >= 100000 and max < 500000 and count > 500:
                            limit = 700000
                            self.adjust_objectid_sequence(table_name, limit)
                        elif max >= 500000 and max < 1100000 and count > 1000:
                            limit = 1500000
                            self.adjust_objectid_sequence(table_name, limit)
                        else:
                            pass
            return True
        
        except Exception as error:
            self.extra_tables.append(table_name)
            return False


    # bu fonksiyon sql komutları kullnarak tabloya kolum ekliyor.
    # eğer arcpy ile oluşturulmuş bir tablo veya feature class'a kolon eklenicekse bi sonrarki fonksiyon kullanulması daha faydalı olur.
    def add_column(self, table, column) -> bool:

        query = f'ALTER TABLE "{table}" ADD "{column[0]}" {column[1]};'
        self.cursor.execute(query)
        print(query, "\n")
        self.conn.commit()
        return True
        

    # bu fonksiyon arcpy komutları kullnarak tabloya kolum ekliyor.
    def add_column_using_arcpy(self, table_name, columns) -> bool:
        
        arcpy.env.workspace = arcpy.os.path.join(MAINPATH, self.ProjectFile)
        full_path = arcpy.os.path.join(arcpy.env.workspace, table_name)
        fieldsinSource = arcpy.ListFields(full_path)
        for col_name, col_type in columns:
            field_names = [field.name for field in fieldsinSource]
            if col_name not in field_names:
                if len(col_name) > 31:
                    print(f"'{col_name}' kolonu çok uzun olduğu için '{table_name}' tablosuna eklenemedi")
                    continue
                if col_name.lower() != "objectid":
                    try:
                        if self.data_type_mappings_esri[col_type] == "TEXT":
                            arcpy.management.AddField(
                                table_name,
                                col_name,
                                self.data_type_mappings_esri[col_type],
                                field_alias = " ".join(col_name.split("_")),
                                field_length = 5000
                            )
                        else:
                            arcpy.management.AddField(
                                table_name,
                                col_name,
                                self.data_type_mappings_esri[col_type],
                                field_alias = " ".join(col_name.split("_"))
                            )
                        print(f"\t'{col_name}' kolonu '{table_name}' tablosuna eklendi")

                    except arcpy.ExecuteError:
                        print(f"{col_name}' kolonu '{table_name}' tablosuna eklenemedi.\nLütfen arcgisi kapatıp tekrar deneyin")
                
                    except Exception as error:
                        self.extra_tables.append(table_name)
                    
        return True
        

    # bu fonksiyon sql kullanarak kolon düşürür.
    def drop_column(self, table, column) -> bool:
        
        if column != "gdb_geomattr_data":
            query = f'ALTER TABLE "{table}" DROP COLUMN "{column}";'
            print(query)
            self.cursor.execute(query)
            print(query, "\n")
            self.conn.commit()
            return True
 
    # bu fonksiyon da kolon düşürür ama arcpy kullanarak yapar.
    def drop_column_using_arcpy(self, table_name, columns):

        arcpy.env.workspace = arcpy.os.path.join(MAINPATH, self.ProjectFile)
        full_path = arcpy.os.path.join(arcpy.env.workspace, table_name)
        fieldsinSource = arcpy.ListFields(full_path)
        for col_name in columns:
            field_names = [field.name for field in fieldsinSource]
            if col_name in field_names:
                if col_name != "objectid":
                    try:
                        arcpy.management.DeleteField(table_name, [col_name])
                        print(f"\t'{col_name}' kolonu '{table_name}' tablosundan silindi")

                    except arcpy.ExecuteError as err:
                        print(f"{col_name}' kolonu '{table_name}' tablosundan silinemedi.\n{err}\nLütfen arcgisi kapatıp tekrar deneyin")

                    except Exception as error:
                        self.extra_tables.append(table_name)


    def delete_table(self, table) -> bool:
        
        query = f'DROP TABLE "{table}";'
        self.cursor.execute(query)
        print(query, "\n")
        self.conn.commit()
        return True
    
    # bu fonkiyon ile bir tablonun herhangibir kolumunun veritipii içindeki verilerler birlikte değiştirilebilir.
    def alter_datatype(self, table_name, column_name, new_data_type) -> bool:

        query = f'ALTER TABLE "{table_name}" ALTER COLUMN "{column_name}" TYPE {new_data_type} USING "{column_name}"::{new_data_type};'
        self.cursor.execute(query)
        print(query, "\n")
        self.conn.commit()
        return True


    def Close_DB(self) -> None:
        
        self.cursor.close()
        self.conn.close()
        





if __name__ == "__main__":

    tables = ["deneme_tablo"]
    
    TARGET = {
            "database": "database",
            "user": "user",
            "password": "password",
            "host": "localhost",
            "port": "5432",
            "schema": "public"
        }

    Target_db = Connect(TARGET, extra_tables, dtypes, dtype_es, schema=TARGET["schema"], tables=tables)
    tables = Target_db.get_table_column_names()
    print(tables)





