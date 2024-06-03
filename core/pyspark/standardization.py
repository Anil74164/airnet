from AirVeda import AirVeda
 
def replace_columns(df_to_rename):
    column_replacements = AirVeda.get_ColumnReplacement()
    for old_name, new_name in column_replacements.items():
        df_to_rename = df_to_rename.withColumnRenamed(old_name, new_name)
    return df_to_rename
 
def convert_to_dict(df_to_convert):
    return df_to_convert.toPandas().to_dict(orient='list')