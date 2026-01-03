# Python

### Generator Expression
* Generator Expression is a lazy version of list comprehension.
* Instead of building the whole list in memory, it produces values one-by-one only when needed.

* squares = [x*x for x in range(1_000_000)]
    * ⛔ Creates 1,000,000 numbers in memory immediately

* squares = (x*x for x in range(1_000_000))
    * Create a generator object, and 
    * Computes values only when you ask for them 

#### Why this is huge for data engineering (Glue, big files, PDFs, S3)

Generator expressions allow you to:

• Stream files
• Stream database rows
• Process millions of PDFs
• Avoid out-of-memory crashes

Which matters a lot for your Glue ETL jobs.

#### Sort() and Sorted() 
* sort() works for list
* sorted() works for iterables in general (tuples, sets)