import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
import psycopg2
import logging
from urllib.parse import urljoin
from dotenv import load_dotenv
import os


# Loading environment variables from .env file
load_dotenv()

# Configuring logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("ETL")


############################################################################################
# Part 1: ETL Pipeline - Extraction, Transformation and Loading to Staging Table "raw_books"
############################################################################################


def extract_book_data():
    base_url = "http://books.toscrape.com/"
    # listing_url = urljoin(base_url, "catalogue/page-1.html")
    books = []
    page = 1

    while True:
        url = urljoin(base_url, f"catalogue/page-{page}.html")
        logger.info("Scraping listing page: %s", url)
        response = requests.get(url)
        if response.status_code != 200:
            logger.error("Failed to retrieve listing page: %s", url)
            break

        soup = BeautifulSoup(response.text, "html.parser")
        articles = soup.find_all("article", class_="product_pod")
        if not articles:
            break

        for article in articles:
            title = article.h3.a.get("title")
            price_text = article.find("p", class_="price_color").text.strip()
            rating_classes = article.find("p", class_="star-rating").get("class", [])
            rating = next((r for r in rating_classes if r != "star-rating"), None)
            availability = article.find("p", class_="instock availability").text.strip()

            # Getting each book's url (detail page) and thumbnail url
            relative_link = article.h3.a.get("href")
            book_url = urljoin(url, relative_link)
            thumbnail_relative = article.find("img").get("src")
            book_thumbnail_url = urljoin(url, thumbnail_relative)

            # Visiting the book's url (detail page) for additional fields
            book_response = requests.get(book_url)
            if book_response.status_code != 200:
                logger.error("Failed to retrieve book detail page: %s", book_url)
                continue
            book_soup = BeautifulSoup(book_response.text, "html.parser")

            # Category: found in breadcrumb navigation (3rd item)
            breadcrumb_items = book_soup.find("ul", class_="breadcrumb").find_all("li")
            category = breadcrumb_items[2].text.strip() if len(breadcrumb_items) >= 3 else None

            # Product description
            desc_div = book_soup.find("div", id="product_description")
            if desc_div:
                desc_paragraph = desc_div.find_next_sibling("p")
                product_description = desc_paragraph.text.strip() if desc_paragraph else None
            else:
                product_description = None


            # Extracting table data
            table = book_soup.find("table", class_="table table-striped")
            table_data = {}
            if table:
                for row in table.find_all("tr"):
                    header = row.find("th").text.strip()
                    value = row.find("td").text.strip()
                    table_data[header] = value

            upc = table_data.get("UPC")
            product_type = table_data.get("Product Type")
            price_excl_tax = table_data.get("Price (excl. tax)")
            price_incl_tax = table_data.get("Price (incl. tax)")
            tax = table_data.get("Tax")
            availability_detail = table_data.get("Availability")
            available_quantity = None
            if availability_detail:
                qty_match = re.search(r'(\d+)', availability_detail)
                if qty_match:
                    available_quantity = int(qty_match.group(1))
            no_of_reviews = table_data.get("Number of reviews")
            try:
                no_of_reviews = int(no_of_reviews) if no_of_reviews is not None else 0
            except Exception as e:
                logger.error("Error converting number of reviews for book '%s': %s", title, e)
                no_of_reviews = 0

            # the complete record
            book_info = {
                "title": title,
                "price": price_text,
                "rating": rating,
                "availability": availability,
                "category": category,
                "book_url": book_url,
                "book_thumbnail_url": book_thumbnail_url,
                "product_description": product_description,
                "upc": upc,
                "product_type": product_type,
                "price_excl_tax": price_excl_tax,
                "price_incl_tax": price_incl_tax,
                "tax": tax,
                "available_quantity": available_quantity,
                "no_of_reviews": no_of_reviews
            }
            books.append(book_info)

        # Pagination: Checking if a "next" button exists
        next_button = soup.find("li", class_="next")
        if next_button:
            page += 1
        else:
            break

    return books



def transform_data(books):
    df = pd.DataFrame(books)

    # dropping duplicate records
    df.drop_duplicates(inplace=True)

    # converting a price string to a numeric value in pounds
    def convert_to_pounds(price_str):
        if pd.isna(price_str) or not price_str:
            return None
        # Removing unexpected characters like "Â" and extra whitespace
        price_str = price_str.replace("Â", "").strip()

        if price_str.startswith('£'):
            try:
                return float(price_str.replace('£', ''))
            except Exception as e:
                logger.error("Error converting price %s: %s", price_str, e)
                return None
        else:
            # if the price is in USD ($), converting using a fixed conversion rate
            if price_str.startswith('$'):
                try:
                    value = float(price_str.replace('$', ''))
                    conversion_rate = 0.75  # assumed conversion rate from USD to GBP
                    return round(value * conversion_rate, 2)
                except Exception as e:
                    logger.error("Error converting price %s: %s", price_str, e)
                    return None
            else:
                logger.error("Unknown currency in price: %s", price_str)
                return None

    # Converting all price-related fields
    price_fields = ['price', 'price_excl_tax', 'price_incl_tax', 'tax']
    for field in price_fields:
        df[field + '_converted'] = df[field].apply(convert_to_pounds)
    for field in price_fields:
        df[field] = df[field + '_converted']
        df.drop(columns=[field + '_converted'], inplace=True)

    # Dropping records if title is null
    df = df.dropna(subset=["title"])

    # if price is missing or null, filling with category-wise average
    for field in ['price', 'price_excl_tax', 'price_incl_tax', 'tax']:
        df[field] = df.groupby('category')[field].transform(lambda x: x.fillna(x.mean()))

    # mapping textual ratings to numbers; if null, setting to 0
    rating_map = {"One": 1, "Two": 2, "Three": 3, "Four": 4, "Five": 5}
    df['rating'] = df['rating'].map(rating_map).fillna(0).astype(int)

    # Transforming "availability": if "In stock" is present, making it True, otherwise False.
    df['availability'] = df['availability'].apply(lambda x: True if x and "In stock" in x else False)

    # If category is missing or null, setting default value "Others"
    df['category'] = df['category'].fillna("Others")

    # If product_type is missing or null, setting default value "Books"
    df['product_type'] = df['product_type'].fillna("Books")

    # Ensuring available_quantity is numeric
    df['available_quantity'] = pd.to_numeric(df['available_quantity'], errors='coerce').fillna(0).astype(int)

    # Checking consistency: price_incl_tax - tax should equal price_excl_tax (within a tolerance)
    df['price_diff'] = df['price_incl_tax'] - df['tax'] - df['price_excl_tax']
    tolerance = 0.01
    inconsistent_rows = df[abs(df['price_diff']) > tolerance]
    if not inconsistent_rows.empty:
        logger.warning("Price inconsistency found in %d records (price_incl_tax - tax != price_excl_tax)", len(inconsistent_rows))
        #if inconsistent then setting price_excl_tax = price_incl_tax - tax only to inconsistent rows
        df.loc[abs(df['price_diff']) > tolerance, 'price_excl_tax'] = df['price_incl_tax'] - df['tax']
    df.drop(columns=['price_diff'], inplace=True)

    # Checking consistency: price should be equal to price_incl_tax
    df['price_mismatch'] = abs(df['price'] - df['price_incl_tax'])
    inconsistent_price = df[df['price_mismatch'] > tolerance]
    if not inconsistent_price.empty:
        logger.warning("Mismatch found: 'price' and 'price_incl_tax' differ in %d records", len(inconsistent_price))
        # if inconsistent then setting price = price_incl_tax only to inconsistent rows
        df.loc[df['price_mismatch'] > tolerance, 'price'] = df['price_incl_tax']
    df.drop(columns=['price_mismatch'], inplace=True)

    return df



def load_to_postgres(df):
    # postgreSQL connection
    conn = psycopg2.connect(
        host=os.getenv('HOST'),
        port=os.getenv('PORT'),
        database=os.getenv('DATABASE'),
        user=os.getenv('USER'),
        password=os.getenv('PASSWORD')
    )
    cur = conn.cursor()

    # creating staging table "raw_books" to load the data
    create_table_query = """
    CREATE TABLE IF NOT EXISTS raw_books (
        id SERIAL PRIMARY KEY,
        title VARCHAR(255) NOT NULL,
        price NUMERIC(10, 2),
        rating INTEGER,
        availability BOOLEAN,
        category VARCHAR(100),
        book_url TEXT,
        book_thumbnail_url TEXT,
        product_description TEXT,
        upc VARCHAR(50),
        product_type VARCHAR(50),
        price_excl_tax NUMERIC(10, 2),
        price_incl_tax NUMERIC(10, 2),
        tax NUMERIC(10, 2),
        available_quantity INTEGER,
        no_of_reviews INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    cur.execute(create_table_query)
    conn.commit()

    insert_query = """
    INSERT INTO raw_books (title, price, rating, availability, category, book_url, book_thumbnail_url, product_description, upc, product_type, price_excl_tax, price_incl_tax, tax, available_quantity, no_of_reviews)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """

    for _, row in df.iterrows():
        cur.execute(insert_query, (
            row['title'],
            row['price'],
            row['rating'],
            row['availability'],
            row['category'],
            row['book_url'],
            row['book_thumbnail_url'],
            row['product_description'],
            row['upc'],
            row['product_type'],
            row['price_excl_tax'],
            row['price_incl_tax'],
            row['tax'],
            row['available_quantity'],
            row['no_of_reviews']
        ))

    conn.commit()
    cur.close()
    conn.close()
    logger.info("Data loaded into staging table 'raw_books' successfully.")



################################################
# Part 2: Data Warehouse Creation and Population
################################################


def build_data_warehouse():
    """
    creating the data warehouse (star-schema: dimension and fact tables)
    and then populating them using data from the staging table 'raw_books'
    """
    conn = psycopg2.connect(
        host=os.getenv('HOST'),
        port=os.getenv('PORT'),
        database=os.getenv('DATABASE'),
        user=os.getenv('USER'),
        password=os.getenv('PASSWORD')
    )
    cur = conn.cursor()

    # creating dimension table: product_type
    cur.execute("""
    CREATE TABLE IF NOT EXISTS product_type (
        id SERIAL PRIMARY KEY,
        product_type_name VARCHAR(50) NOT NULL UNIQUE
    );
    """)
    # creating dimension table: books_details
    cur.execute("""
    CREATE TABLE IF NOT EXISTS books_details (
        id SERIAL PRIMARY KEY,
        title VARCHAR(255) NOT NULL,
        upc VARCHAR(50),
        product_description TEXT,
        book_url TEXT,
        book_thumbnail_url TEXT,
        no_of_reviews INTEGER
    );
    """)
    # creating dimension table: category_info
    cur.execute("""
    CREATE TABLE IF NOT EXISTS category_info (
        id SERIAL PRIMARY KEY,
        category_name VARCHAR(100) NOT NULL UNIQUE
    );
    """)
    # creating dimension table: rating_info
    cur.execute("""
    CREATE TABLE IF NOT EXISTS rating_info (
        id SERIAL PRIMARY KEY,
        rating_value INTEGER NOT NULL UNIQUE
    );
    """)
    # creating dimension table: availability_info
    cur.execute("""
    CREATE TABLE IF NOT EXISTS availability_info (
        id SERIAL PRIMARY KEY,
        availability_status VARCHAR(50) NOT NULL UNIQUE
    );
    """)

    # creating fact table: books_fact
    cur.execute("""
    CREATE TABLE IF NOT EXISTS books_fact (
        id SERIAL PRIMARY KEY,
        books_details_id INTEGER REFERENCES books_details(id),
        price NUMERIC,
        price_excl_tax NUMERIC,
        price_incl_tax NUMERIC,
        tax NUMERIC,
        available_quantity INTEGER,
        category_id INTEGER REFERENCES category_info(id),
        rating_id INTEGER REFERENCES rating_info(id),
        availability_id INTEGER REFERENCES availability_info(id),
        product_type_id INTEGER REFERENCES product_type(id),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)
    conn.commit()

    # populating the dimension tables from "raw_books" table

    # populating product_type dimension
    cur.execute("""
    INSERT INTO product_type (product_type_name)
    SELECT DISTINCT product_type FROM raw_books
    ON CONFLICT (product_type_name) DO NOTHING;
    """)
    # populating books_details dimension
    cur.execute("""
    INSERT INTO books_details (title, upc, product_description, book_url, book_thumbnail_url, no_of_reviews)
    SELECT rb.title,
            rb.upc,
            rb.product_description,
            rb.book_url,
            rb.book_thumbnail_url,
            rb.no_of_reviews
    FROM raw_books rb;
    """)
    # populating category_info dimension
    cur.execute("""
    INSERT INTO category_info (category_name)
    SELECT DISTINCT category FROM raw_books
    ON CONFLICT (category_name) DO NOTHING;
    """)
    # populating rating_info dimension
    cur.execute("""
    INSERT INTO rating_info (rating_value)
    SELECT DISTINCT rating FROM raw_books ORDER BY rating ASC
    ON CONFLICT (rating_value) DO NOTHING;
    """)
    # populating availability_info dimension
    cur.execute("""
    INSERT INTO availability_info (availability_status)
    SELECT DISTINCT CASE WHEN availability THEN 'In Stock' ELSE 'Out of Stock' END FROM raw_books
    ON CONFLICT (availability_status) DO NOTHING;
    """)
    conn.commit()

    # Populating the Fact Table "books_fact" by joining staging data with dimension tables
    insert_fact_query = """
    INSERT INTO books_fact (
        books_details_id, price, price_excl_tax, price_incl_tax, tax, available_quantity,
        category_id, rating_id, availability_id, product_type_id
    )
    SELECT
        bd.id,
        rb.price,
        rb.price_excl_tax,
        rb.price_incl_tax,
        rb.tax,
        rb.available_quantity,
        ci.id,
        ri.id,
        ai.id,
        pt.id
    FROM raw_books rb
    LEFT JOIN books_details bd ON rb.title = bd.title AND rb.upc = bd.upc
    LEFT JOIN category_info ci ON rb.category = ci.category_name
    LEFT JOIN rating_info ri ON rb.rating = ri.rating_value
    LEFT JOIN availability_info ai ON (CASE WHEN rb.availability THEN 'In Stock' ELSE 'Out of Stock' END) = ai.availability_status
    LEFT JOIN product_type pt ON rb.product_type = pt.product_type_name;
    """
    cur.execute(insert_fact_query)
    conn.commit()

    cur.close()
    conn.close()
    logger.info("Data Warehouse (Star-Schema) built and populated successfully.")



########################################################
# Main Function: Executing ETL & Building Data Warehouse
########################################################


def main():
    logger.info("Starting ETL process")
    # Extraction
    books = extract_book_data()
    logger.info("Extracted %d records", len(books))

    # Transformation
    df = transform_data(books)
    logger.info("Transformed data, resulting in %d records", len(df))

    # Loading into staging table "raw_books"
    load_to_postgres(df)
    logger.info("ETL process completed successfully.")

    # Building and populating the data warehouse from staging table "raw_books"
    build_data_warehouse()
    logger.info("Data Warehousing process completed successfully.")

if __name__ == "__main__":
    main()



