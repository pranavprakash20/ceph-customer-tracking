import pygsheets
import pandas as pd
import psycopg


class CustBugsToDoc:
    def __init__(self, db_name, db_user, db_password, db_host, db_port, g_doc, g_key):
        self.db_name = db_name
        self.db_user = db_user
        self.db_password = db_password
        self.db_host = db_host
        self.db_port = db_port
        self.db_conn = None
        self.data = None
        self.gc = None
        self.g_key = g_key
        self.g_doc = g_doc
        self.g_wks = None

        self._initialise_db_conn()

    def update_data(self):
        print("Getting all data")
        query = f"SELECT * FROM ceph_customer_bugs where target >= '5' ORDER BY reported_date DESC"
        cursor = self.db_conn.cursor()
        cursor.execute(query)
        self.data = cursor.fetchall()
        self.push_data_to_docs()

    def push_data_to_docs(self):
        self._authorize_google_sheet_conn()
        sh = self.gc.open('Ceph Customer Bugs')
        self.g_wks = sh[0]
        cols = ['Time', 'Bug_id', 'Summary', 'Version', 'Target Version', 'Component', 'Severity', 'Assignee', 'QA_Contact',
                'State', 'Resolution', 'Closed Loop Completed', 'qa_whiteboard', 'Triaged', 'Reported By', 'Customer Data']

        df = pd.DataFrame(data=self.data, columns=cols)
        self.g_wks.set_dataframe(df, (1, 1))
        self.g_wks.set_basic_filter()

        # Create hyperlink
        self._create_hyperlink()

    def _authorize_google_sheet_conn(self):
        self.gc = pygsheets.authorize(service_file=self.g_key)

    def _create_hyperlink(self):
        rows = self.g_wks.get_all_values()
        i = 2
        for _ in rows:
            id = self.g_wks.get_value(f"B{i}")
            self.g_wks.update_value(f"B{i}", f'=HYPERLINK("https://bugzilla.redhat.com/show_bug.cgi?id={id}", "{id}")')
            i += 1


    def _initialise_db_conn (self):
        self.db_conn = psycopg.connect(
            dbname=self.db_name,
            user=self.db_user,
            password=self.db_password,
            host=self.db_host,
            port=self.db_port
        )
        self.db_conn.autocommit = True


obj = CustBugsToDoc(db_name="postgres",
                    db_user="postgres",
                    db_password="admin",
                    db_host="10.0.209.12",
                    db_port="5432",
                    g_doc="Ceph Customer Bugs",
                    g_key="ceph-customer-bz-key.json")

# create a key for accessing google doc. Ref https://www.vmix.com/help26/GoogleAPIKey.html
obj.update_data()
