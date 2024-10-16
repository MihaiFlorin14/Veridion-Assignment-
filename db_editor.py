# This script compares and updates records in the Google and Website tables based on the reference Facebook table. 
# For each user in the Facebook table, it fetches matching records from Google and Website tables, compares specific fields, 
# and updates Google and Website with Facebook data if there are discrepancies.

import sqlite3

# Path to the database
db_path = ''

# Function to compare and update tables within the database
def compare_and_update_tables(limit=100):
    # Connect to the single database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Fetch all users from Facebook (reference) table, limiting to the first `limit` records
    cursor.execute("SELECT * FROM Facebook LIMIT ?", (limit,))
    facebook_users = cursor.fetchall()

    # Define a mapping for the `Facebook` table fields
    facebook_fields = {
        'name': 9,           
        'address': 1,        
        'phone': 11,         
        'email': 8           
    }

    # Define a mapping for the `Website` table fields
    website_fields = {
        'site_name': 9,      
        'main_city': 5,      
        'phone': 8,          
        'legal_name': 4      
    }

    # Define a mapping for the `Google` table fields
    google_fields = {
        'name': 6,           
        'address': 1,        
        'phone': 7,          
        'city': 3,           
        'region_name': 12,   
        'country_name': 5,   
        'zip_code': 14       
    }

    # Iterate through each user in the Facebook (reference) table
    for user in facebook_users:
        name = user[facebook_fields['name']]  

        # Fetch the corresponding user from Google and Website tables
        cursor.execute("SELECT * FROM Google WHERE name = ?", (name,))
        google_user = cursor.fetchone()

        cursor.execute("SELECT * FROM Website WHERE site_name = ?", (name,))
        website_user = cursor.fetchone()

        if google_user and website_user:
            # Compare data and auto-complete Google and Website tables
            google_updates = {}
            website_updates = {}

            # Store old and new values for Google and Website
            old_google_data = {}
            new_google_data = {}
            old_website_data = {}
            new_website_data = {}

            # Compare Facebook and Google data, exclude 'email' because Google has no email field
            for field, index in facebook_fields.items():
                if field == 'email':  
                    continue

                facebook_value = user[index]
                google_value = google_user[google_fields[field]] if field in google_fields else None

                # Update Google if values differ and the Facebook value is valid (non-NULL)
                if google_value != facebook_value and facebook_value is not None:
                    google_updates[field] = facebook_value
                    old_google_data[field] = google_value  
                    new_google_data[field] = facebook_value  

            # Compare Facebook and Website data (mapping corresponding fields)
            for field, index in website_fields.items():
                facebook_value = user[facebook_fields[field]] if field in facebook_fields else None
                website_value = website_user[index] if website_user else None

                # Update Website if values differ and the Facebook value is valid (non-NULL)
                if website_value != facebook_value and facebook_value is not None:
                    website_updates[field] = facebook_value
                    old_website_data[field] = website_value  
                    new_website_data[field] = facebook_value  

            # Only print if there are updates
            if google_updates or website_updates:
                print(f"\n===== Updates for user: {name} =====")

                # Print all old and new Google values
                if google_updates:
                    print("\nGoogle Table:")
                    print("Old Google Data:", old_google_data)
                    print("New Google Data:", new_google_data)

                # Print all old and new Website values
                if website_updates:
                    print("\nWebsite Table:")
                    print("Old Website Data:", old_website_data)
                    print("New Website Data:", new_website_data)

            # Update Google table if there are differences
            if google_updates:
                update_query = "UPDATE Google SET " + ', '.join([f"{k} = ?" for k in google_updates.keys()]) + " WHERE name = ?"
                cursor.execute(update_query, (*google_updates.values(), name))
                print(f"\nGoogle table updated for user {name}.")

            # Update Website table if there are differences
            if website_updates:
                update_query = "UPDATE Website SET " + ', '.join([f"{k} = ?" for k in website_updates.keys()]) + " WHERE site_name = ?"
                cursor.execute(update_query, (*website_updates.values(), name))
                print(f"\nWebsite table updated for user {name}.")

    # Commit changes and close connection
    conn.commit()
    conn.close()

# Run the function, limit to the first 100 records
compare_and_update_tables()
