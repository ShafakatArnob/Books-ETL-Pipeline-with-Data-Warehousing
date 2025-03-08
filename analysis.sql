--raw_books analysis--
select * from raw_books;
select count(*) from raw_books;

select distinct upc from raw_books;
select distinct price from raw_books;
select distinct rating from raw_books;
select distinct availability from raw_books;
select distinct category from raw_books;
select distinct product_type from raw_books;
select distinct tax from raw_books;
select count(*) from raw_books where price_excl_tax = price_incl_tax;
select count(*) from raw_books where price = price_incl_tax;
select count(*) from raw_books where price = price_excl_tax;
select distinct available_quantity from raw_books;
select distinct no_of_reviews from raw_books;


--warehouse tables analysis--
select * from books_fact;
select * from books_details;
select * from category_info;
select * from rating_info;
select * from availability_info;
select * from product_type;
