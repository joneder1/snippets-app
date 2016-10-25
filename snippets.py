import logging
import argparse
import psycopg2

# Set the log output file, and the log level
logging.basicConfig(filename="snippets.log", level=logging.DEBUG)
logging.debug("Connecting to PostgreSQL")
connection = psycopg2.connect(database="snippets")
logging.debug("Database connection established.")

def put(name, snippet):
    """Store a snippet with an associated name."""
    logging.info("Storing snippet {!r}: {!r}".format(name, snippet))
    with connection.cursor() as cursor:
        try:
            cursor.execute("insert into snippets values (%s, %s)", (name, snippet))
        except psycopg2.IntegrityError:
            connection.rollback()
            cursor.execute("update snippets set message=%s where keyword=%s", (snippet, name))
    connection.commit()
    logging.debug("Snippet stored successfully.")
    return name, snippet
    
def get(name):
    """Retrieve the snippet with a given name.
    If there is no such snippet, return '404: Snippet Not Found'.
    Returns the snippet.
    """
    logging.info("Getting snippet {!r}".format(name))
    with connection, connection.cursor() as cursor:
        cursor.execute("select message from snippets where keyword=%s", (name,))
        row = cursor.fetchone()
    logging.debug("Snippet retrieved successfully.")
    if not row:
        # No snippet was found with that name.
        return "404: Snippet Not Found"
    return row[0]
    
def catalog():
    """Query the available keywords from the snippets table."""
    logging.info("Querying the database")
    with connection.cursor() as cursor:
        cursor.execute("select * from snippets order by keyword")
        rows = cursor.fetchall()
    for row in rows:
        keyword = row[0]
        print (keyword)
    logging.debug("Query complete")
    
def search(string):
    """Return list of snippets which contain a given string anywhere in their messages"""
    logging.info("Searching snippets for {}".format(string))
    with connection.cursor() as cursor:
        cursor.execute("select * from snippets where message like '%%'||%s||'%%'", (string,))
        rows = cursor.fetchall()
        for row in rows:
            message = row[1]
            print (message)
    logging.debug("Search complete")
    
def main():
    """Main function"""
    logging.info("Constructing parser")
    parser = argparse.ArgumentParser(description="Store and retrieve snippets of text")

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Subparser for the put command
    logging.debug("Constructing put subparser")
    put_parser = subparsers.add_parser("put", help="Store a snippet")
    put_parser.add_argument("name", help="Name of the snippet")
    put_parser.add_argument("snippet", help="Snippet text")
    # Subparser for the get command
    logging.debug("Constructing get subparser")
    get_parser = subparsers.add_parser("get", help="retrieve a snippet")
    get_parser.add_argument("name", help="Name of the snippet")
    # Subparser for the catalog command
    logging.debug("Constructing catalog subparser")
    subparsers.add_parser("catalog", help="Query snippet keywords")
    # Subparser for the search command
    logging.debug("Constructing search subparser")
    search_parser = subparsers.add_parser("search", help="Search snippets for a string")
    search_parser.add_argument("string", help="The string you are searching for")
    arguments = parser.parse_args()
    # Convert parsed arguments from Namespace to dictionary
    arguments = vars(arguments)
    command = arguments.pop("command")

    if command == "put":
        name, snippet = put(**arguments)
        print("Stored {!r} as {!r}".format(snippet, name))
    elif command == "get":
        snippet = get(**arguments)
        print("Retrieved snippet: {!r}".format(snippet))
    elif command == "catalog":
        catalog()
        print("Retrieved keywords")
    elif command == "search":
        search(**arguments)
        print("Search complete")
        # receiving Found <function search at 0x7fa710afff28> in these messages, do I need to use __repr__?
        print("Found {!r} in these messages".format(search))

if __name__ == "__main__":
    main()