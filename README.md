### analyst-collective/models

A collection of model definitions for common data sets in SQL

### contributing

##### sql construction conventions
- first layer should simply set fields and table / schema
- second layer should be filter. if no records to be filtered out, simply implement as select *.
- third layer should be transformations. this could include datatype conversions, mapping, and other simple transformations to make the data more standardized and consumable. *all transformations must meet the strict definition of universal applicability.*
- all files should be DDL (should create permanent database objects, not just execute queries)
- all files should contain a single ddl operation

### questions

- need to create a destination schema for views created.
  - should be separate schema for each source system or all together in a single schema?
  - should scripts automatically drop / recreate schemas? much cleaner but high potential for fuckup by users not paying attention.
  - should the schema for the intermediate views (base, filtered, transformed) be separate from the schema for the final views? i think so...
- how should unit / integration / regression testing work? the last two, especially, are a huge deal.
- what's the best way to execute a bunch of sql statements in a row even with the sql source being in independent files?
- how do these get documented in an SEO-friendly way? coming across one of these in github will scare off most medium-technical biz users...