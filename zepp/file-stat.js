/**
 * Check master node
 */

sc.master

/**
 * Number of articles that are not Live till date
 */
println("Number of articles that are not Live till date")
val notlive = artread.filter(line => line).filter(x=>x.contains("agbladet/404")).count()


//All Statisitic about Artread File
val artread = sc.textFile("/Users/sureshkumarmukhiya/Downloads/basis/dagbladet/clicklog")


//display Number of rows in the file
println("Number of Rows in Clicklog File")
val rows = artread.count

