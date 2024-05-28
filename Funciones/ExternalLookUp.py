

def ExternalExport(conn,tabla,branch,columns):
    columns_str = ",".join(columns)
    cursor = conn.cursor()
    query = f"SELECT {columns_str} FROM {tabla} where branch = {branch}"
    cursor.execute(query)
    df = cursor.fetchall()
    cursor.close
    return df