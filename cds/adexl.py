from lxml import etree
import os
import sqlite3
import pandas.io.sql

class ADE_XL_Result(object):
    def __init__(self, directory):
        self.directory = directory

        self.setupdb = self.xmldata()

        self._load_tests()
        self._load_history()
        
    def check(self):
        ## FIXME, check consistency
        return True
    
    def xmldata(self):
        """Return ADE-XL XML data etree"""
        datafile = os.path.join(self.directory, "data.sdb")
        return etree.parse(datafile)

    def expand_path(self, path):
        """Expand paths relative to result directory"""
        return path.replace('$AXL_SETUPDB_DIR', self.directory)

    def get_history_entry_names(self):
        return [he.name for he in self.history]

    def get_history_entry(self, name):
        for h_entry in self.history:
            if h_entry.name == name:
                return h_entry

    def _load_tests(self):
        self.tests = [ADE_XL_Test(elem) 
                      for elem in self.setupdb.findall('active/tests/test')]

    def _load_history(self):
        self.history = [ADE_XL_HistoryEntry(elem) 
                        for elem in self.setupdb.findall('history/historyentry')]

class ADE_XL_Test(object):
    def __init__(self, elem):
        self.elem = elem

        self.name = elem.text.strip()

        self._load_tooloptions()
        self._load_outputs()

    def _load_outputs(self):
        self.outputs = [ADE_XL_TestOutput(elem.text.strip())
                        for elem in self.elem.findall('outputs/output')]

    def _load_tooloptions(self):
        ## Load tool options
        tooloptions = {}
        for tooloption in self.elem.findall('tooloptions/option'):
            value = tooloption.find('./value')
            tooloptions[tooloption.text.strip()] = value.text.strip()

        self.tooloptions = tooloptions

    def get_state_dir(self, result):
        return os.path.join(*([result.expand_path(self.tooloptions['path']), 
                               self.cellview[0],
                               self.cellview[1],
                               self.simulator,
                               self.tooloptions['state']
                               ]))
    
    def get_state_file(self, filename, result):
        with open(os.path.join(self.get_state_dir(result), filename)) as f:
            return f.read()

    @property
    def cellview(self):
         return [self.tooloptions['lib'], 
                 self.tooloptions['cell'], 
                 self.tooloptions['view']]

    @property
    def simulator(self):
         return self.tooloptions['sim']

class ADE_XL_TestOutput(object):
    def __init__(self, name):
        self.name = name

class ADE_XL_HistoryEntry(object):
    def __init__(self, elem):
        self.elem = elem

        self._load_history_entry()
        
    def _load_history_entry(self):
        self.name = self.elem.text.strip()
        self.simresults = self.elem.find('./simresults').text.strip()

    def get_result_db(self, result):
        if self.simresults:
            sqlitedbfile = result.expand_path(self.simresults)
            return sqlite3.connect(sqlitedbfile)

    def get_all_results(self, result):
        """Get all results as pandas dataframe"""
        
        query = """SELECT 
                       test.name, resultValue.pointID, result.name, resultValue.value
                   FROM 
                       resultValue
                   INNER JOIN 
                       result ON result.resultID = resultValue.resultID
                   INNER JOIN
                       test ON test.testID = result.testID
                """
        
        conn = self.get_result_db(result)
        return pandas.io.sql.read_frame(query, conn)

    def get_parameters(self, pointid, result):
        query = """SELECT
                      parameter.name, parameterValue.value
                   FROM
                      parameterValue
                   INNER JOIN 
                      parameter ON parameter.parameterID = parameterValue.parameterID
                   WHERE
                      parameterValue.pointID = %d""" % pointid
        conn = self.get_result_db(result)
        return pandas.io.sql.read_frame(query, conn)
        
if __name__ == '__main__':
    r = ADE_XL_Result(os.path.join(os.path.dirname(__file__), 'test', 'data', 'adexl'))
    
    test = r.tests[0]

    statedir = test.get_state_dir(r)

    history = r.history
    
    print "Tests:", [test.name for test in r.tests]
    print "History:", [he.name for he in r.history]

    resultdb = history[0].get_result_db(r)

    tables = resultdb.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()

    print "Tables: %s" % tables

    query = """SELECT 
                   test.name, resultValue.pointID, result.name, resultValue.value
               FROM 
                   resultValue
               INNER JOIN 
                   result ON result.resultID = resultValue.resultID
               INNER JOIN
                   test ON test.testID = result.testID
            """
    
    for historyentry in r.history:
        print historyentry.get_parameters(1, r)
        print historyentry.get_all_results(r)

