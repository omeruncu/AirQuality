import psycopg2
from psycopg2 import sql
from src.utils.logger import LoggerFactory
from src.utils.error_handler import ErrorHandler

class PostgreSQLConnector:
    def __init__(self, db_url):
        self.db_url = db_url
        self.logger = LoggerFactory.get_logger(self.__class__.__name__)
        if not self.db_url:
            self.logger.error("Database URL is not set. Please check your configuration.")
        else:
            # Hassas bilgileri gizle
            safe_url = self.db_url.replace(self.db_url.split('@')[0], '***:***')
            self.logger.info(f"Database URL: {safe_url}")

    @ErrorHandler.handle_errors("PostgreSQLConnector")
    def connect(self):
        if not self.db_url:
            raise ValueError("Database URL is not set. Please check your configuration.")
        self.logger.info("Attempting to connect to the database...")
        try:
            conn = psycopg2.connect(self.db_url)
            self.logger.info("Successfully connected to the database.")
            return conn
        except psycopg2.OperationalError as e:
            self.logger.error(f"Failed to connect to the database: {str(e)}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error while connecting to the database: {str(e)}")
            raise

    @ErrorHandler.handle_errors("PostgreSQLConnector")
    def insert_data(self, data):
        with self.connect() as conn:
            with conn.cursor() as cur:
                insert_query = sql.SQL("""
                    INSERT INTO air_quality 
                    (city, aqi, timestamp, carbon_intensity, iaqi_co, iaqi_dew, iaqi_h, iaqi_no2, iaqi_o3, iaqi_p, iaqi_pm10, iaqi_pm25, iaqi_so2, iaqi_t, iaqi_w)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """)
                cur.execute(insert_query, (
                    data['city'],
                    data['aqi'],
                    data['timestamp'],
                    data['carbon_intensity'],
                    data['iaqi'].get('co', {}).get('v'),
                    data['iaqi'].get('dew', {}).get('v'),
                    data['iaqi'].get('h', {}).get('v'),
                    data['iaqi'].get('no2', {}).get('v'),
                    data['iaqi'].get('o3', {}).get('v'),
                    data['iaqi'].get('p', {}).get('v'),
                    data['iaqi'].get('pm10', {}).get('v'),
                    data['iaqi'].get('pm25', {}).get('v'),
                    data['iaqi'].get('so2', {}).get('v'),
                    data['iaqi'].get('t', {}).get('v'),
                    data['iaqi'].get('w', {}).get('v')
                ))
                conn.commit()
        self.logger.info("Data inserted into PostgreSQL successfully")

    
    @ErrorHandler.handle_errors("PostgreSQLConnector")
    def fetch_data(self, limit=100):
        # limit'in bir tamsayı olduğundan emin olalım
        if isinstance(limit, dict):
            self.logger.warning(f"limit bir sözlük olarak geçirildi: {limit}. Varsayılan değer 100 kullanılıyor.")
            limit = 100
        elif not isinstance(limit, int):
            try:
                limit = int(limit)
            except (ValueError, TypeError):
                self.logger.warning(f"Geçersiz limit değeri: {limit}. Varsayılan değer 100 kullanılıyor.")
                limit = 100

        query = f"SELECT * FROM air_quality ORDER BY timestamp DESC LIMIT {limit}"
        self.logger.info(f"Executing query: {query}")  # Log the query for debugging
        try:
            with self.connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(query)
                    columns = [desc[0] for desc in cur.description]
                    results = cur.fetchall()
                    return [dict(zip(columns, row)) for row in results]
        except Exception as e:
            self.logger.error(f"Error in fetch_data: {str(e)}")
            raise
            
    def test_connection(self):
        try:
            with self.connect() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    result = cur.fetchone()
                    if result[0] == 1:
                        self.logger.info("Database connection test successful")
                        
                        # Tablo yapısını kontrol et
                        cur.execute("""
                            SELECT column_name, data_type 
                            FROM information_schema.columns
                            WHERE table_name = 'air_quality'
                        """)
                        columns = cur.fetchall()
                        self.logger.info(f"Table structure: {columns}")
                        
                        return True
        except Exception as e:
            self.logger.error(f"Database connection test failed: {str(e)}")
        return False