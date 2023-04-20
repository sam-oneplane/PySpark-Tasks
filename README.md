# Wizdome-Task1 explained, 

### the inpute include 3 .json files 
   1. cves.json     include information about cybersecurity volnerabilities and exposures (cve) .
   2. cpes.json     scheme of products, software and packages (cpe).
   3. cve2cpe.json  help to map cves to cpes

### pyspark flow

  1.  read all json files (cves.json, cpes.json, cve2cpe.json) 
      into dataframes (cves_df, cpes_df, cve2cpe_df)

  2.  perform a cve2cpe_df left join from cpe_df, this means the all cve2cpe_df will be taken 
      with additional information from cpe_df if they are joined, if not, null values will be added.
      
      this is under the join conditions specified in the task.
      
      REMARk : 
            the if condition is done using (A | B) meaning , in OR operation conditiion B is checked
            only if condition A is false (if A is true then A | B is also true)
            in our task for example : 
              ((cve2cpe_df.to_version == "") | (cpes_df.product_version < cve2cpe_df.to_version))
            the condition : (cve2cpe_df.to_version == "") | (cpes_df.product_version < cve2cpe_df.to_version) 
            is checked only if cve2cpe_df.to_version is not empty. 
            
   3.  after join there canbe sevral cpes addressed to the same cve infree translation this means that there could be
       several sottware/ package / product that is related to the same cybersecurity volnerabilitie.
       
       so we perform a groupby on cve and aggregate with collect_list on the cpe.
       
   4.  the recieved dataframe then inner joined with the cves_df to include only the ones that are in 
       cves_df
   
   5. the result dataframe is then written to a json output file.
   
   
