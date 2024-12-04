from fastapi import FastAPI, HTTPException
from app.models.top_ctr_model import TopCTRModel          # Importar la clase TopCTRModel
from app.models.top_product_model import TopProductModel
from app.testdb import get_db_connection
import psycopg2

app = FastAPI(title="AdTech Recommendation API")

@app.get("/recommendations/{advertiser}/{model}")
async def get_recommendations(advertiser: str, model: str):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        if model == "TopProduct":
            cur.execute("""
                SELECT product_id, views FROM TopProduct WHERE advertiser_id = %s;
            """, (advertiser,))
            recommendations = cur.fetchall()
            # Formatear la respuesta
            recommendations = [{"product_id": row[0], "views": row[1]} for row in recommendations]
        
        elif model == "TopCTR":
            cur.execute("""
                SELECT product_id, clicks, impressions, ctr FROM TopCTR WHERE advertiser_id = %s;
            """, (advertiser,))
            recommendations = cur.fetchall()
            # Formatear la respuesta
            recommendations = [{"product_id": row[0], "clicks": row[1], "impressions": row[2], "ctr": row[3]} for row in recommendations]
        
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
            SELECT product_id, views FROM TopProduct WHERE advertiser_id = %s;
        """, (advertiser,))
        top_product_history = cur.fetchall()
        
        # Obtener el historial de TopCTR
        cur.execute("""
            SELECT product_id, clicks, impressions, ctr FROM TopCTR WHERE advertiser_id = %s;
        """, (advertiser,))
        top_ctr_history = cur.fetchall()

        # Formatear la respuesta
        history = {
            "top_products": [{"product_id": row[0], "views": row[1]} for row in top_product_history],
            "top_ctr": [{"product_id": row[0], "clicks": row[1], "impressions": row[2], "ctr": row[3]} for row in top_ctr_history]
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
            SELECT 
                COUNT(DISTINCT advertiser_id) as advertiser_count_TopCTR,
                (SELECT COUNT(*) FROM TopProduct) + (SELECT COUNT(*) FROM TopCTR) as total_recommendations
            FROM TopProduct;
            SELECT 
                COUNT(DISTINCT advertiser_id) as advertiser_count_TopProduct,
                (SELECT COUNT(*) FROM TopProduct) + (SELECT COUNT(*) FROM TopCTR)
            FROM TopCTR;
        """)
        
        
        stats = cur.fetchall()
        
        return {
            "advertiser_count": stats[0][0],
            "total_recommendations": stats[0][1]
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
    finally:
        cur.close()
        conn.close()