# Sage Web Disabled Visibility

## Description

Flags all items that shouldn't be displayed on the Magento Website.

## Systems
- Sage100
- Akeneo
- Wrike

## Schedule
Daily 11:10PM

## Current Logic

#everything defaults as not disabled and visibility = 
1 = Not Visible Individually
2 = Catalog
3 = Search
4 = Catalog, Search

#Inactive
-Under year Catalog, Search
-1-3 year Search
-Over 3 Year Invisible        

#Inactive, Has Replacement - 
-Under 2 years Catalog, Search
-2-4 year Search
-Over 5 Year Invisible    

#Makes anything invisible with temp disco searchable (will be in catalog too if replacement is active[below])    

#Inactive, with page, has active replacement and activity within last 6 months increases visibility    

#Anything Invisible is disabled    

## Exceptions

No UDF_SPECIALORDER
No Error UDF_DISCONTINUED_STATUS
No / ItemCodes
No -BVA -NOB -EBY