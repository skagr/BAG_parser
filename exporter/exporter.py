"""Export sqlite BAG to csv or other format"""

import csv
from statusbar import StatusUpdater
import utils
from database_sqlite import DatabaseSqlite


class Exporter:
    def __init__(self):
        self.local_db = DatabaseSqlite()
        self.total_adressen = 0

    def __export_to_csv(self, output_filename, headers, sql, update_status=True):
        status = StatusUpdater()

        utils.print_log(f"start: export adressen naar csv file '{output_filename}'")
        self.local_db.check_valid_database()

        file = open(output_filename, "w", newline="", encoding="utf-8")
        writer = csv.writer(file)

        if update_status:
            total_adressen = self.local_db.fetchone("SELECT COUNT(*) FROM adressen;")
            utils.print_log(f"Totaal aantal adressen: {total_adressen}")
            status.start(total_adressen)

        writer.writerow(headers)

        count = 0
        self.local_db.cursor.execute(sql)
        for row in self.local_db.cursor:
            count += 1
            if update_status:
                status.update(count)
            writer.writerow(row)
            # if count > 10000: break  # Debug speedup

        if update_status:
            status.ready()
        utils.print_log("ready: export naar csv")

    def __export_to_oracle(self, output_filename, headers, sql, update_status=True):
        status = StatusUpdater()

        utils.print_log(f"start: export adressen naar csv file '{output_filename}'")
        self.local_db.check_valid_database()

        file = open(output_filename, "w", newline="", encoding="utf-8")
        writer = csv.writer(file)

        if update_status:
            total_adressen = self.local_db.fetchone("SELECT COUNT(*) FROM adressen;")
            utils.print_log(f"Totaal aantal adressen: {total_adressen}")
            status.start(total_adressen)

        writer.writerow(headers)

        count = 0
        self.local_db.cursor.execute(sql)
        for row in self.local_db.cursor:
            count += 1
            if update_status:
                status.update(count)
            writer.writerow(row)
            # if count > 10000: break  # Debug speedup

        if update_status:
            status.ready()
        utils.print_log("ready: export naar csv")

    def export_to_csv(self, output_filename):
        """
        Export the data from the database to a CSV file.

        Args:
            output_filename (str): The name of the output CSV file.

        Returns:
            None
        """
        headers = [
            "straat",
            "huisnummer",
            "toevoeging",
            "postcode",
            "gemeente",
            "woonplaats",
            "provincie",
            "bouwjaar",
            "rd_x",
            "rd_y",
            "latitude",
            "longitude",
            "vloeroppervlakte",
            "gebruiksdoel",
        ]

        sql = """
            SELECT
              o.naam                       AS straat,
              a.huisnummer,
              a.huisletter || a.toevoeging AS toevoeging,
              a.postcode,
              g.naam                       AS gemeente,
              w.naam                       AS woonplaats,
              p.naam                       AS provincie,
              a.bouwjaar,
              a.rd_x,
              a.rd_y,
              a.latitude,
              a.longitude,
              a.oppervlakte                AS vloeroppervlakte,
              a.gebruiksdoel
            FROM adressen a
              LEFT JOIN openbare_ruimten o ON a.openbare_ruimte_id = o.id
              LEFT JOIN gemeenten g        ON a.gemeente_id        = g.id
              LEFT JOIN woonplaatsen w     ON a.woonplaats_id      = w.id
              LEFT JOIN provincies p       ON g.provincie_id       = p.id;"""

        self.__export_to_csv(output_filename, headers, sql)

    def export_to_csv_postcode(self, output_filename):
        """
        Export addresses from the database to a CSV file grouped by postcode4

        Parameters:
            output_filename (str): The name of the CSV file to be created.

        Returns:
            None
        """
        headers = ["straat", "huisnummer", "toevoeging", "postcode", "woonplaats"]

        sql = """
            SELECT
              o.naam                       AS straat,
              a.huisnummer,
              a.huisletter || a.toevoeging AS toevoeging,
              a.postcode,
              w.naam                       AS woonplaats
            FROM adressen a
              LEFT JOIN openbare_ruimten o ON a.openbare_ruimte_id = o.id
              LEFT JOIN woonplaatsen w     ON a.woonplaats_id      = w.id;"""

        self.__export_to_csv(output_filename, headers, sql)

    def export_to_csv_postcode4_stats(self, output_filename):
        """
        Export statistics (center lat/lon, count of addresses) from the database to a CSV file grouped by postcode4

        Parameters:
            output_filename (str): The name of the CSV file to be created.

        Returns:
            None
        """
        headers = [
            "postcode4",
            "center_lat",
            "center_lon",
            "aantal_adressen",
            "woonplaats",
        ]

        sql = """
          SELECT
            SUBSTR(a.postcode, 0, 5) AS pc4,
            AVG(a.latitude)          AS center_lat,
            AVG(a.longitude)         AS center_lon,
            COUNT(1)                 AS aantal_adressen,
            w.naam                   AS woonplaats
          FROM adressen a
            LEFT JOIN woonplaatsen w ON a.woonplaats_id = w.id
          WHERE a.postcode <> ''
          GROUP BY pc4;"""

        self.__export_to_csv(output_filename, headers, sql, False)

    def export_to_csv_postcode5_stats(self, output_filename):
        """
        Export statistics (center lat/lon, count of addresses) from the database to a CSV file grouped by postcode5

        Parameters:
            output_filename (str): The name of the CSV file to be created.

        Returns:
            None
        """
        headers = [
            "postcode5",
            "center_lat",
            "center_lon",
            "aantal_adressen",
            "woonplaats",
        ]

        sql = """
          SELECT
            SUBSTR(a.postcode, 0, 6) AS pc5,
            AVG(a.latitude)          AS center_lat,
            AVG(a.longitude)         AS center_lon,
            COUNT(1)                 AS aantal_adressen,
            w.naam                   AS woonplaats
          FROM adressen a
            LEFT JOIN woonplaatsen w ON a.woonplaats_id = w.id
          WHERE a.postcode <> ''
          GROUP BY pc5;"""

        self.__export_to_csv(output_filename, headers, sql, False)

    def export_to_csv_postcode6_stats(self, output_filename):
        """
        Export statistics (center lat/lon, count of addresses) from the database to a CSV file grouped by postcode

        Parameters:
            output_filename (str): The name of the CSV file to be created.

        Returns:
            None
        """
        headers = [
            "postcode6",
            "center_lat",
            "center_lon",
            "aantal_adressen",
            "woonplaats",
        ]

        sql = """
          SELECT
            a.postcode       AS pc6,
            AVG(a.latitude)  AS center_lat,
            AVG(a.longitude) AS center_lon,
            COUNT(1)         AS aantal_adressen,
            w.naam           AS woonplaats
          FROM adressen a
            LEFT JOIN woonplaatsen w ON a.woonplaats_id = w.id
          WHERE a.postcode <> ''
          GROUP BY pc6;"""

        self.__export_to_csv(output_filename, headers, sql, False)
