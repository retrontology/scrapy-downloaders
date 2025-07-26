from style import convert_map_to_relative
import mysql.connector


MYSQL_HOST="10.42.0.2"
MYSQL_PORT="3306"
MYSQL_USER="atlas_user"
MYSQL_PASS="vmT36HT0UWhvOeHS7UIcNHyvSOztrtnz"
SOURCE_DB="omniatlas2"
DESTINATION_DB="omniatlas3"


def main():
    conn = mysql.connector.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASS,
    )
    
    cursor_destination = conn.cursor()
    cursor_source = conn.cursor()

    cursor_destination.execute(f"CREATE TABLE IF NOT EXISTS {DESTINATION_DB}.atlas_frame (id VARCHAR(36) PRIMARY KEY NOT NULL, region VARCHAR(255) NOT NULL, date DATE NOT NULL, title VARCHAR(255) NOT NULL, description TEXT NOT NULL, url VARCHAR(255) NOT NULL)")
    cursor_destination.execute(f"CREATE TABLE IF NOT EXISTS {DESTINATION_DB}.atlas_data (id VARCHAR(36) PRIMARY KEY NOT NULL, data MEDIUMTEXT NOT NULL, FOREIGN KEY (id) REFERENCES {DESTINATION_DB}.atlas_frame(id) ON DELETE CASCADE)")
    conn.commit()

    cursor_source.execute(f"SELECT * FROM {SOURCE_DB}.atlas_frame")
    frames = cursor_source.fetchall()
    for frame in frames:
        cursor_destination.execute(f"INSERT INTO {DESTINATION_DB}.atlas_frame (id, region, date, title, description, url) VALUES (%s, %s, %s, %s, %s, %s)", frame)
    conn.commit()

    cursor_source.execute(f"SELECT * FROM {SOURCE_DB}.atlas_data")
    frames = cursor_source.fetchall()
    for frame in frames:
        id, data = frame
        new_data = convert_map_to_relative(data)
        cursor_destination.execute(f"INSERT INTO {DESTINATION_DB}.atlas_data (id, data) VALUES (%s, %s)", (id, new_data))
    conn.commit()

    cursor_source.close()
    cursor_destination.close()
    conn.close()


if __name__ == "__main__":
    main()
