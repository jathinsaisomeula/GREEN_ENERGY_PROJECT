SELECT 
 Country ,
 SUM (`Green_Jobs_Created`) AS no_of_green_jobs ,
 `Sustainability_score`,
  Date
 FROM `sacred-pipe-454410-p7.green_energy.sustainability` 
 GROUP BY 
  Country , 
  Date ,
 `Sustainability_score` ;