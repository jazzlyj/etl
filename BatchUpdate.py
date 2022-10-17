import pandas as pd

def update_to_sql(df, table_name, key_name):
    a = []
    table = table_name
    primary_key = key_name
    temp_table = f"{table_name}_temporary_table"
    for col in df.columns:
        if col == primary_key:
            continue
        a.append(f'"{col}"=s."{col}"')
    df.to_sql(temp_table, engine, if_exists='replace', index=False)
    update_stmt_1 = f'UPDATE public."{table}" f '
    update_stmt_2 = "SET "
    update_stmt_3 = ", ".join(a)
    update_stmt_4 = f' FROM public."{table}" t '
    update_stmt_5 = f' INNER JOIN (SELECT * FROM public."{temp_table}") AS s ON s."{primary_key}"=t."{primary_key}" '
    update_stmt_6 = f' Where f."{primary_key}"=s."{primary_key}" '
    update_stmt_7 = update_stmt_1 + update_stmt_2 + update_stmt_3 + update_stmt_4 + update_stmt_5 +  update_stmt_6 +";"
    print(update_stmt_7)
    with engine.begin() as cnx:
        cnx.execute(update_stmt_7)