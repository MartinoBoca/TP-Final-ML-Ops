from fastapi import FastAPI, HTTPException
import psycopg2

# Conexión a la base de datos
def get_db_connection():
    try:
        conn = psycopg2.connect(
            host="grupo-15-rds.cf4i6e6cwv74.us-east-1.rds.amazonaws.com",
            database="postgres",
            user="postgres",
            password="nvmQVqg3d46tCJa",
            port=5432
            
        )
        return conn
    except Exception as e:
        print(f"Error al conectar a la base de datos: {e}")
        return None

app = FastAPI(title="AdTech Recommendation API")

@app.get("/recommendations/{advertiser}/{model}")
async def get_recommendations(advertiser: str, model: str):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        if model == "TopProducts":
            cur.execute("""
                SELECT product_id, views FROM top_products WHERE advertiser_id = %s AND date = (
                SELECT MAX(date) FROM top_products WHERE advertiser_id = %s) ORDER BY views DESC;
            """, (advertiser, advertiser))
            recommendations = cur.fetchall()
            # Formatear la respuesta
            recommendations = [{"product_id": row[0], "views": row[1]} for row in recommendations]
        
        elif model == "TopCTR":
            cur.execute("""
                SELECT product_id, ctr FROM top_ctr_products WHERE advertiser_id = %s AND date = (
                SELECT MAX(date) FROM top_ctr_products WHERE advertiser_id = %s) ORDER BY ctr DESC;
            """, (advertiser, advertiser))
            recommendations = cur.fetchall()
            # Formatear la respuesta
            recommendations = [{"product_id": row[0], "ctr": row[1]} for row in recommendations]
        
        else:
            raise HTTPException(status_code=400, detail="Invalid model specified")

        cur.close()
        conn.close()
        
        return {"recommendations": recommendations}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/history/{advertiser}")
async def get_history(advertiser: str):
    conn = get_db_connection()
    if conn is None:
        raise HTTPException(status_code=500, detail="No se pudo conectar a la base de datos.")

    cur = conn.cursor()
    try:
        # Obtener el historial de TopProduct
        cur.execute("""
            WITH RankedProducts AS (
            SELECT product_id, views, date,
            ROW_NUMBER() OVER (PARTITION BY date ORDER BY views DESC) AS rank
            FROM top_products WHERE advertiser_id = %s AND date >= 
            ( SELECT MAX(date) - INTERVAL '6 days' 
            FROM top_products WHERE advertiser_id = %s) 
            )
            SELECT product_id, views, date
            FROM RankedProducts WHERE rank = 1
            ORDER BY date DESC;
        """, (advertiser, advertiser))
        top_product_history = cur.fetchall()
        
        # Obtener el historial de TopCTR
        cur.execute("""
            WITH RankedProducts AS (
            SELECT product_id, ctr, date,
            ROW_NUMBER() OVER (PARTITION BY date ORDER BY ctr DESC) AS rank
            FROM top_ctr_products WHERE advertiser_id = %s AND date >= (
            SELECT MAX(date) - INTERVAL '6 days' 
            FROM top_ctr_products WHERE advertiser_id = %s)
            )
            SELECT product_id, ctr, date
            FROM RankedProducts WHERE rank = 1
            ORDER BY date DESC;
        """, (advertiser, advertiser))
        top_ctr_history = cur.fetchall()

        # Formatear la respuesta
        history = {
            "top_products": [{"product_id": row[0], "views": row[1], "date": row[2]} for row in top_product_history],
            "top_ctr": [{"product_id": row[0], "ctr": row[1], "date": row[2]} for row in top_ctr_history]
        }

        return {"history": history}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
    finally:
        cur.close()
        conn.close()

@app.get("/stats")
async def get_stats():
    conn = get_db_connection()
    if conn is None:
        raise HTTPException(status_code=500, detail="No se pudo conectar a la base de datos.")

    cur = conn.cursor()
    try:
        # Contar anunciantes distintos y total de recomendaciones
        cur.execute("""
            SELECT COUNT(DISTINCT advertiser_id) as advertiser_count_TopProduct FROM top_products;
            SELECT COUNT(DISTINCT advertiser_id) as advertiser_count_TopCTR FROM top_ctr_products;
        """)
        
        
        stats = cur.fetchall()
        
        return {
            "advertiser_count": stats[0][0],
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
    finally:
        cur.close()
        conn.close()