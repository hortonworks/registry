#!/usr/bin/python
# -*- coding: utf-8 -*-
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import os
import sys
import glob
import traceback
from xml.etree.ElementTree import ElementTree


def print_detail_information(file_path, failure_cases):
    print "-" * 50
    for testcase in failure_cases:
        print "classname: %s / testname: %s" % (testcase.get("classname"), testcase.get("name"))
    print "-" * 50
    print "Printing output..."
    output_file_path = file_path.replace("TEST-", "").replace(".xml", "-output.txt")
    if os.path.exists(output_file_path):
        with open(output_file_path, "r") as fr:
            print fr.read()
    else:
        print "No output file for the test case. desired output file path: %s" % output_file_path

    print "-" * 50


def print_error_reports_from_report_file(file_path):
    tree = ElementTree()
    try:
        tree.parse(file_path)
    except:
        print "-" * 50
        print "Error parsing %s" % file_path
        with open(file_path, "r") as fr:
            print fr.read()
        print "-" * 50
        return

    testcases = tree.findall(".//testcase")
    failure_cases = []
    for testcase in testcases:
        error = testcase.find("error")
        fail = testcase.find("fail")
        failure = testcase.find("failure")

        if error is not None or fail is not None or failure is not None:
            failure_cases.append(testcase)

    if len(failure_cases) > 0:
        print_detail_information(file_path, failure_cases)


def main(report_dir_path):
    for test_report in glob.iglob(report_dir_path + '/TEST-*.xml'):
        file_path = os.path.abspath(test_report)
        try:
            print "Checking %s" % test_report
            print_error_reports_from_report_file(file_path)
        except Exception, e:
            print "Error while reading report file, %s" % file_path
            print "Exception: %s" % e
            traceback.print_exc()


if __name__ == "__main__":
    if sys.argv < 2:
        print "Usage: %s [report dir path]" % sys.argv[0]
        sys.exit(1)

    main(sys.argv[1])
