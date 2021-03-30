# imports needed
import pandas as pd
import numpy as np
import os
from env import host, user, password

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MinMaxScaler


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
    # SQL query that joins all of the tables together from the 'zillow' database
    # Tweaked to include Ravinder's insights
    sql_query = """
                SELECT prop.*, 
                       pred.logerror, 
                       pred.transactiondate, 
                       air.airconditioningdesc, 
                       arch.architecturalstyledesc, 
                       build.buildingclassdesc, 
                       heat.heatingorsystemdesc, 
                       landuse.propertylandusedesc, 
                       story.storydesc, 
                       construct.typeconstructiondesc 
				FROM   properties_2017 prop  
                INNER JOIN (SELECT parcelid,
       					           logerror,
                                   Max(transactiondate) transactiondate 
                            FROM   predictions_2017 
                            GROUP  BY parcelid, logerror) pred
                         USING (parcelid) 
                LEFT JOIN airconditioningtype air USING (airconditioningtypeid) 
                LEFT JOIN architecturalstyletype arch USING (architecturalstyletypeid) 
                LEFT JOIN buildingclasstype build USING (buildingclasstypeid) 
                LEFT JOIN heatingorsystemtype heat USING (heatingorsystemtypeid) 
                LEFT JOIN propertylandusetype landuse USING (propertylandusetypeid) 
                LEFT JOIN storytype story USING (storytypeid) 
                LEFT JOIN typeconstructiontype construct USING (typeconstructiontypeid) 
                WHERE prop.latitude IS NOT NULL 
                      AND prop.longitude IS NOT NULL;
                """
    return pd.read_sql(sql_query,get_connection('zillow'))


def wrangle_zillow():
    df = pd.read_csv('zillow.csv')
    
    # Restrict df to only properties that meet single-use criteria
    single_use = [261, 262, 263, 264, 266, 268, 273, 276, 279]
    df = df[df.propertylandusetypeid.isin(single_use)]
    
    # Filter those properties without at least 1 bath & bed and 500 sqft area
    df = df[(df.bedroomcnt > 0) & (df.bathroomcnt > 0) & ((df.unitcnt<=1)|df.unitcnt.isnull())\
            & (df.calculatedfinishedsquarefeet>500)]

    # Drop columns and rows based on a predetermined criteria
    df = handle_missing_values(df)
    
    # Add column for counties
    df['county'] = np.where(df.fips == 6037, 'Los_Angeles',
                           np.where(df.fips == 6059, 'Orange', 
                                   'Ventura'))
    
    # Drop unnecessary/redundant columns
    df = df.drop(['id',
       'calculatedbathnbr', 'finishedsquarefeet12', 'fullbathcnt', 'heatingorsystemtypeid'
       ,'propertycountylandusecode', 'propertylandusetypeid','propertyzoningdesc', 
        'censustractandblock', 'propertylandusedesc', 'heatingorsystemdesc'],axis=1)
    
    # Replace nulls in unitcnt with 1
    df.unitcnt.fillna(1, inplace = True)
    
    # Replace nulls with median values for select columns
    df.lotsizesquarefeet.fillna(7315, inplace = True)
    df.buildingqualitytypeid.fillna(6.0, inplace = True)
    
    # Drop any remaining nulls
    df = df.dropna()
    
    # Columns that need to be adjusted for outliers
    df = df[df.taxvaluedollarcnt < 4_500_000]
    df[df.calculatedfinishedsquarefeet < 8000]
    
    return df

def handle_missing_values(df, prop_required_column = .5, prop_required_row = .70):
	#Function will drop rows or columns based on the percent of values that are missing
	#handle_missing_values(df, prop_required_column, prop_required_row
    threshold = int(round(prop_required_column*len(df.index),0))
    df.dropna(axis=1, thresh=threshold, inplace=True)
    threshold = int(round(prop_required_row*len(df.columns),0))
    df.dropna(axis=0, thresh=threshold, inplace=True)
    return df



######################## Mall Portion ############################################

def get_mall_data():
    '''
    This function uses the SQL query from below and specifies the database to use
    '''
    # SQL query that joins all of the tables together from the 'mall_customers' database     
    sql_query = """
                SELECT * 
                FROM customers 
                """
    return pd.read_sql(sql_query,get_connection('mall_customers'), index_col='customer_id')


def outlier_function(df, cols, k):
	#Function to detect and handle outliers using IQR
    for col in df[cols]:
        q1 = df.annual_income.quantile(0.25)
        q3 = df.annual_income.quantile(0.75)
        iqr = q3 - q1
        upper_bound =  q3 + k * iqr
        lower_bound =  q1 - k * iqr     
        df = df[(df[col] < upper_bound) & (df[col] > lower_bound)]
    return df


def wrangle_mall_df():
    
    # acquire data
    mall_df = get_mall_data()
    
    # handle outliers
    mall_df = outlier_function(mall_df, ['age', 'spending_score', 'annual_income'], 1.5)
    
    # get dummy for gender column
    dummy_df = pd.get_dummies(mall_df.gender, drop_first=True)
    mall_df = pd.concat([mall_df, dummy_df], axis=1).drop(columns = ['gender'])

    # split the data in train, validate and test
    train, test = train_test_split(mall_df, train_size = 0.8, random_state = 123)
    train, validate = train_test_split(train, train_size = 0.75, random_state = 123)
    
    return min_max_scaler(train, validate, test)