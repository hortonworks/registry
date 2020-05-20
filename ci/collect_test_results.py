#!/usr/bin/env python

import fnmatch
import os
import xml.etree.ElementTree as ET

print('<results>')
for root, directories, filenames in os.walk('.'):
  reports_dir = '/target/surefire-reports'
  if reports_dir in root:
    module = root[2: len(root) - len(reports_dir)]
    print('<module>')
    print('<name>{}</name>'.format(module))
    print('<testsuites>')
    for filename in fnmatch.filter(filenames, '*.xml'):
      file_path = os.path.join(root, filename)
      xml = ET.parse(file_path)
      testsuite = xml.getroot()
      print('<testsuite>')
      print('<name>{}</name>'.format(testsuite.get('name')))
      print('<info>')
      print('<tests>{}</tests>'.format(testsuite.get('tests')))
      print('<errors>{}</errors>'.format(testsuite.get('errors')))
      print('<failures>{}</failures>'.format(testsuite.get('failures')))
      print('<skipped>{}</skipped>'.format(testsuite.get('skipped')))
      print('<time>{}</time>'.format(testsuite.get('time')))
      print('</info>')
      print('<testcases>')
      for testcase in xml.findall('testcase'):
        result_state = 'success'
        result_type = result_text = ''
        # skipped
        skipped = testcase.find('skipped')
        if skipped is not None:
          result_state = result_type = 'skipped'
          result_text = skipped.text
        # failure
        failure = testcase.find('failure')
        if failure is not None:
          result_state = 'failure'
          result_type = failure.get('type')
          result_text = failure.text
        # error
        error = testcase.find('error')
        if error is not None:
          result_state = 'error'
          result_type = error.get('type')
          result_text = error.text
        print('<testcase>')
        print('<info>')
        print('<state><{}/></state>'.format(result_state))
        print('<name>{}</name>'.format(testcase.get('name')))
        print('<time>{}</time>'.format(testcase.get('time')))
        print('</info>')
        # system-out
        sysout_elem = testcase.find('system-out')
        # system-err
        syserr_elem = testcase.find('system-err')
        if result_state in ['error', 'failure']:
          print('<result>')
          print('<type>{}</type>'.format(result_type or ''))
          print('<text>{}</text>'.format(result_text or ''))
          if sysout_elem is not None:
            print('<sysout>')
            print(sysout_elem.text or '')
            print('</sysout>')
          if syserr_elem is not None:
            print('<syserr>')
            print(syserr_elem.syserr_text or '')
            print('</syserr>')
          print('</result>')
        print('</testcase>')
      print('</testcases>')
      print('</testsuite>')
    print('</testsuites>')
    print('</module>')
print('</results>')
