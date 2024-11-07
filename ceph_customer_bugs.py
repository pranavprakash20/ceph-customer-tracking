import json

import bugzilla
import psycopg
from datetime import datetime


class FetchCustomerData:

    def __init__ (self, url, api_key, db_name, db_user, db_password, db_host, db_port):
        self.api_key = api_key
        self.url = url
        self.db_name = db_name
        self.db_user = db_user
        self.db_password = db_password
        self.db_host = db_host
        self.db_port = db_port
        self.bz_api = self._initialise_api_req()
        self.db_conn = None
        self.cols = ["reported_date", "bug_id", "summary", "version", "target", "component", "severity", "assignee",
                     "qa_contact", "state", "resolution", "closed_loop", "qa_whiteboard", "triaged", "reported_by",
                     "customer_data"]
        self.table_name = "ceph_customer_bugs"
        self._initialise_db_conn()

    def fetch_all_customer_bugs (self):
        """
        Use this method only when you are writing to the DB for first time. It fetches all the customer bugs raised
        so far ana append to DB
        """
        URL = "https://bugzilla.redhat.com/buglist.cgi?bug_status=NEW&bug_status=ASSIGNED&bug_status=POST&bug_status=MODIFIED&bug_status=ON_DEV&bug_status=ON_QA&bug_status=VERIFIED&bug_status=RELEASE_PENDING&bug_status=CLOSED&classification=Red%20Hat%20Storage&columnlist=opendate%2Cproduct%2Ccomponent%2Cassigned_to%2Cqa_contact&f1=external_bugzilla.description&list_id=13431733&o1=substring&order=qa_contact%2C%20&product=Red%20Hat%20Ceph%20Storage&query_format=advanced&v1=Red%20Hat%20Customer%20Portal"
        bugs = self._run_bugzilla_query(URL)
        print(f"Customer Bugs reported in ceph so far : {len(bugs)} ")
        self._get_bugs_data(bugs, verbose=True)
        self.db_conn.close()

    def fetch_customer_bugs(self):
        """
        Master method which fetches the customer bugs in last 24 hrs. The same is appened to the database.
        Also, every existing bz is checked for any changes in the parameters saved
        """
        try:
            # Step 1: Get last 24 hour Bug
            URL = """https://bugzilla.redhat.com/buglist.cgi?bug_status=__open__&classification=Red%20Hat%20Storage&columnlist=opendate%2Cproduct%2Ccomponent%2Cassigned_to%2Cqa_contact&f1=external_bugzilla.description&f2=creation_ts&list_id=13400065&o1=substring&o2=greaterthan&order=qa_contact%2C%20&product=Red%20Hat%20Ceph%20Storage&query_format=advanced&v1=Red%20Hat%20Customer%20Portal&v2=-1D"""
            bugs = self._run_bugzilla_query(URL)
            print(f"Customer Bugs reported in last 24 hours: {len(bugs)} ")
            if len(bugs) > 0:
                self._get_bugs_data(bugs, verbose=True)

            # Step 2 : Now see if the existing bugs have changed (all_open bugs)
            print("Checking if the open bugs have been updated in last 24 hours")
            select_query = f"select * from ceph_customer_bugs where target > '5'"
            bugs_data = self._run_sql_query(select_query)
            print(f"Checking data on {len(bugs_data)} existing bugs")
            self._check_for_data_changes(bugs_data)

            # Step 3: Check for closed loop bugs status
            print("[Closed Loop] Checking if the closed loop pending bugs with resolution [ERRATA or CURRENTRELEASE) "
                  "and High or Urgent Severity have been updated in last 24 hours")
            select_query = f"select * from ceph_customer_bugs where closed_loop = 'False' and state = " \
                           f"'CLOSED' and (resolution = 'ERRATA' or resolution = 'CURRENTRELEASE') and (severity = " \
                           f"'urgent' OR severity = 'high');"
            bugs_data = self._run_sql_query(select_query)
            print(f"Checking data on {len(bugs_data)} existing bugs")
            self._check_for_data_changes(bugs_data)
        except Exception as e:
            print(f"FAILED!! Error {str(e)}")
        finally:
            # Close the db_conn
            print("Closing DB connection")
            self.db_conn.close()

    def _get_bugs_data(self, bugs, verbose=False):
        """
        Get all bugs data
        Args:
            bugs (list(Bugzilla)): List of bugzilla bugs
        """
        try:
            ctr = 0
            for bug in bugs:
                b = self.bz_api.getbug(bug.id)
                data = self._get_bug_data(b, verbose=verbose)
                self._insert_db(data)
                ctr += 1
                print(f"Total rows inserted : {ctr}/{len(bugs)}")
            print("\n\n")
            print("**********************")
            print("  All Data inserted")
            print("**********************")
        except Exception as e:
            print(f"Failed to get details {e}")

    def _get_bug_data(self, b, verbose=False):
        """
        Get all the required data specific to the given bug
        Args:
            b (bugzilla): Bugzilla object containing bug info
        """

        polarion_requirement = False
        qe_test_coverage = False
        created_time = b.creation_time.value
        dt = datetime.strptime(str(created_time), '%Y%m%dT%H:%M:%S')
        triaged = "True" if "triaged" in b.cf_ibm_storage_qa_whiteboard.lower() else "False"
        for section in b.flags:
            if "qe_test_coverage" in section['name']:
                if section["status"] == "-":
                    polarion_requirement = True
                qe_test_coverage = True
                if section["status"] == "?":
                    print("qe_test_coverage ?, marking closed loop false")
                    qe_test_coverage = False
        if not polarion_requirement:
            for section in b.external_bugs:
                match = ["Polarion Requirement", "Polarion test case"]
                if any(m in section['type']['description'] for m in match):
                    polarion_requirement = True
        is_closed_loop = all([qe_test_coverage, polarion_requirement])
        customer_details = ""
        for item in b.external_bugs:
            if (item["ext_description"]) and item["ext_description"]!= "None":
                customer_details += (item["ext_description"])
        customer_details = customer_details.replace("'", "")
        if verbose:
            print("Fetched bug #%s:" % b.id)
            print("  Product            = %s" % b.product)
            print("  Component          = %s" % b.component)
            print("  Status             = %s" % b.status)
            print("  Resolution         = %s" % b.resolution)
            print("  Version            = %s" % b.version)
            print("  Summary            = %s" % b.summary)
            print("  Time               = %s" % dt)
            print("  Severity           = %s" % b.severity)
            print("  Target release     = %s" % b.target_release)
            print("  QA contact         = %s" % b.qa_contact)
            print("  Assignee           = %s" % b.assigned_to)
            print("  QA Whiteboard      = %s" % b.cf_ibm_storage_qa_whiteboard.lower())
            print("  Triaged            = %s" % triaged)
            print(f" Closed Loop        = {is_closed_loop} ")
            print(f" Reported By        = {b.creator} ")
            print(f" Customer Details   = {customer_details} ")

        summary = b.summary.replace('"', '')
        summary = summary.replace("'", "")
        data = [f"'{dt}'", f"'{b.id}'", f"'{summary}'", f"'{b.version}'", f"'{b.target_release[0]}'",
                f"'{b.component}'", f"'{b.severity}'", f"'{b.assigned_to}'", f"'{b.qa_contact}'", f"'{b.status}'",
                f"'{b.resolution}'", f"'{str(is_closed_loop)}'", f"'{str(b.cf_ibm_storage_qa_whiteboard.lower())}'",
                f"'{triaged}'", f"'{b.creator}'",  f"'{customer_details}'"]
        return data

    def _check_for_data_changes(self, bugs_data):
        ctr = 0
        total = len(bugs_data)
        updated = False
        for entry in bugs_data:
            try:
                # Get the bug id from the db__entry
                b = self.bz_api.getbug(int(entry[1]))
                bug_data = self._get_bug_data(b)

                # Ignore the fields ("reported_date", "bug_id", "summary")
                existing_data = ' '.join(entry[3:]).replace("'", "").split()
                bug_data = ' '.join(bug_data[3:]).replace("'", "").split()
                if bug_data != existing_data:
                    print(f"Change seen in Bug: #{b.id}")
                    print(existing_data)
                    print(bug_data)
                    print("\n")
                    self._update_row_in_db(b)
                    updated = True
                else:
                    print(f"No change in bug : # {b.id}\n")
                print(f"Completed : {ctr}/{total}")
                ctr += 1
            except Exception as e:
                print(f"Failed in _check_for_data_changes {e}")

        if updated:
            print("The DB has been updated with recent changes")
        else:
            print("The exiting data is up to date")



    def _run_bugzilla_query(self, URL):
        query = self.bz_api.url_to_query(URL)
        query["include_fields"] = ["id", "summary"]
        query["ids_only"] = True
        bugs = self.bz_api.query(query)
        return bugs

    def _run_sql_query(self, query):
        cursor = self.db_conn.cursor()
        cursor.execute(query)
        return cursor.fetchall()

    def _insert_db(self, data):
        cmd = f"""INSERT INTO {self.table_name} ({",".join(self.cols)}) VALUES ({",".join(data)})"""
        self.db_conn.cursor().execute(cmd)

    def _update_row_in_db(self, b):
        # Step 1: Delete the row
        self.db_conn.cursor().execute(f"DELETE FROM {self.table_name} where bug_id = '{b.id}'")

        # Step 2: Insert the updated data into db
        data = self._get_bug_data(b)
        self._insert_db(data)

    def _initialise_api_req (self):
        return bugzilla.Bugzilla(self.url, api_key=self.api_key)

    def _initialise_db_conn (self):
        self.db_conn = psycopg.connect(
            dbname=self.db_name,
            user=self.db_user,
            password=self.db_password,
            host=self.db_host,
            port=self.db_port
        )
        self.db_conn.autocommit = True


obj = FetchCustomerData(url="bugzilla.redhat.com",
                        api_key="qwr8Ciw5t37lT9CvplWJbFrdlvsXjRAv1MZpWfSC",
                        db_name="postgres",
                        db_user="postgres",
                        db_password="admin",
                        db_host="10.0.209.12",
                        db_port="5432")
print("\n\n\n===================================================")
print("Execution started : ", datetime.now())
obj.fetch_all_customer_bugs()
# obj.fetch_customer_bugs()
print("Execution ended : ", datetime.now())
print("===================================================")
