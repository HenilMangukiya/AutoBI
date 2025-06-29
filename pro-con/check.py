def get_selected_cities_from_cell():
    import gspread
    from oauth2client.service_account import ServiceAccountCredentials

    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
    client = gspread.authorize(creds)

    sheet = client.open("weather_city_config").worksheet("Cities")

    # ⚠ Read from cell B1 (not the column!)
    cell_value = sheet.acell("B2").value

    print("Raw B1 content:", repr(cell_value))  # Debug line

    if not cell_value or "Selected" in cell_value:
        print("❌ Cell B1 does not contain city names.")
        return []

    cities = [c.strip() for c in cell_value.split(",") if c.strip()]
    return cities

# Demo
if __name__ == "__main__":
    cities = get_selected_cities_from_cell()
    print("✅ Selected Cities:")
    for city in cities:
        print("-", city)
