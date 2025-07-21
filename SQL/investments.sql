SELECT
  Country,
  SUM(`Investment_in_Renewables_Billion_USD`) AS total_investments,
  AVG(`Installed_Renewable_Capacity_GW`) AS avg_installed_renewable_capacity 
FROM `sacred-pipe-454410-p7.green_energy.sustainability`
GROUP BY
  Country 
ORDER BY
  total_investments DESC;