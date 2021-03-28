

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