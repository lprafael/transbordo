import psycopg2
from datetime import datetime

DB_TRANSACCIONES = {
    'host': 'replicatransacciones.vmt.gov.py',
    'port': '5435',
    'dbname': 'transacciones',
    'user': 'devmt',
    'password': 'FootgearBlinkedDigFreewillStricken',
    "options": "-c statement_timeout=60000"
}

def check():
    try:
        conn = psycopg2.connect(**DB_TRANSACCIONES)
        cur = conn.cursor()
        
        date_start = '2025-12-11'
        date_end = '2025-12-12'
        
        print(f"Checking date: {date_start}")
        
        cur.execute(f"SELECT COUNT(*) FROM c_transacciones WHERE fechahoraevento >= '{date_start}' AND fechahoraevento < '{date_end}'")
        count = cur.fetchone()[0]
        print(f"Total transactions: {count}")
        
        query_transfers = f"""
        SELECT COUNT(*) 
        FROM c_transacciones 
        WHERE fechahoraevento >= '{date_start}' 
          AND fechahoraevento < '{date_end}' 
          AND idproducto IN ('4d4f')
          AND tipoevento = 4 
          AND (
              (entidad = '0002' AND númerotransbordos IN (1,5,6,9,10))
              OR
              (entidad = '0003' AND númerotransbordos IN (1,2))
          )
        """
        cur.execute(query_transfers)
        transfers = cur.fetchone()[0]
        print(f"Transfers detected: {transfers}")
        
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check()
