import psycopg2

# Conexión a la base de datos
def get_db_connection():
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="testdb",  # Cambia esto por el nombre real de tu base de datos
            user="postgres",     # Cambia esto por tu usuario real
            password="admin"     # Cambia esto por tu contraseña real
        )
        return conn
    except Exception as e:
        print(f"Error al conectar a la base de datos: {e}")
        return None

def create_tables():
    conn = get_db_connection()
    if conn is None:
        print("No se pudo establecer la conexión a la base de datos.")
        return

    cur = conn.cursor()

    # Crear tabla TopCTR
    cur.execute("""
        CREATE TABLE IF NOT EXISTS TopCTR (
            advertiser_id VARCHAR(255) NOT NULL,
            product_id VARCHAR(255) NOT NULL,
            clicks INT DEFAULT 0,
            impressions INT DEFAULT 0,
            ctr FLOAT DEFAULT 0,
            PRIMARY KEY (advertiser_id, product_id)
        );
    """)

    # Crear tabla TopProduct
    cur.execute("""
        CREATE TABLE IF NOT EXISTS TopProduct (
            advertiser_id VARCHAR(255) NOT NULL,
            product_id VARCHAR(255) NOT NULL,
            views INT DEFAULT 0,
            PRIMARY KEY (advertiser_id, product_id)
        );
    """)

    conn.commit()
    cur.close()
    conn.close()

def insert_data():
    conn = get_db_connection()
    if conn is None:
        print("No se pudo establecer la conexión a la base de datos.")
        return

    cur = conn.cursor()

    # Insertar datos en la tabla TopCTR
    topctr_data = [
        ('2WPF1NXECF3G6NUMWDO7', 'urms05', 1, 1, 1.0),
        ('5E325T5HYL61QSABVR5V', 'm6uuv9', 1, 3, 0.3333333333333333),
        ('62FIK8F2YT8JSFDBLEC9', '1jvqaq', 1, 1, 1.0),
        ('6X20RDH567MX2X3TXYJ7', 'g1lslm', 1, 2, 0.5),
        ('8C88YB6E8YCGWU07HA7A', '7436n0', 1, 1, 1.0)
    ]

    for row in topctr_data:
        cur.execute("""
            INSERT INTO TopCTR (advertiser_id, product_id, clicks, impressions, ctr)
            VALUES (%s, %s, %s, %s, %s);
        """, row)

    # Insertar datos en la tabla TopProduct
    topproduct_data = [
        ('2WPF1NXECF3G6NUMWDO7', 'dlir67', 5),
        ('2WPF1NXECF3G6NUMWDO7', 'k2umzv', 4),
        ('2WPF1NXECF3G6NUMWDO7', 's2vf7b', 4),
        ('2WPF1NXECF3G6NUMWDO7', '1ch7g8', 3),
        ('2WPF1NXECF3G6NUMWDO7', '1gjnn4', 3)
    ]

    for row in topproduct_data:
        cur.execute("""
            INSERT INTO TopProduct (advertiser_id, product_id, views)
            VALUES (%s, %s, %s);
        """, row)

    conn.commit()  # Confirmar los cambios
    cur.close()
    conn.close()

if __name__ == "__main__":
    create_tables()  # Crear tablas
    insert_data()     # Insertar datos