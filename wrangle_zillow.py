# imports needed
import pandas as pd
import numpy as np
import os
from env import host, user, password

def get_connection(db, user=user, host=host, password=password):
    '''
    This function uses my info from my env file to
    create a connection url to access the Codeup db.
    '''
    return f'mysql+pymysql://{user}:{password}@{host}/{db}'
    

def get_zillow_data():
    '''
    This function uses the SQL query from below and specifies the database to use
    '''
    # SQL query that joins all of the tables together from the 'telco_churn' database     
    sql_query = """
                SELECT parcelid, count(transactiondate), transactiondate
				FROM predictions_2017
				JOIN properties_2017 USING (parcelid)
				JOIN unique_properties USING (parcelid)
				LEFT JOIN propertylandusetype USING (propertylandusetypeid)
				LEFT JOIN airconditioningtype USING (airconditioningtypeid)
				LEFT JOIN architecturalstyletype USING (architecturalstyletypeid)
				LEFT JOIN buildingclasstype USING (buildingclasstypeid)
				LEFT JOIN heatingorsystemtype USING (heatingorsystemtypeid)
				LEFT JOIN storytype USING (storytypeid)
				LEFT JOIN typeconstructiontype USING (typeconstructiontypeid)
				GROUP BY parcelid, transactiondate
				ORDER BY count(transactiondate) ASC;
                """
    return pd.read_sql(sql_query,get_connection('zillow'))



