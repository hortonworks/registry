#!/usr/bin/env python

import os
import xml.etree.ElementTree as ET

print('<results>')
abspath = os.path.abspath('.')
checkstyle_result = 'checkstyle-result.xml'
for root, directories, filenames in os.walk('.'):
  target_dir = '/build/reports/checkstyle'
  if root.endswith(target_dir) and checkstyle_result in filenames:
    module = root[2: len(root) - len(target_dir)]
    file_path = os.path.join(root, checkstyle_result)
    xml = ET.parse(file_path)
    checkstyle = xml.getroot()
    if checkstyle.find('file/error') is not None:
      print('<module>')
      print('<name>{}</name>'.format(module))
      for elem in checkstyle.findall('file'):
        file_name = elem.get('name').replace(abspath + '/', '')
        errors = elem.findall('error')
        if len(errors) > 0:
          print('<file>')
          print('<name>{}</name>'.format(file_name))
          for error in errors:
            print('<error>')
            print('<src>{}:{}</src>'.format(os.path.basename(file_name), error.get('line')))
            print('<message>{}</message>'.format(error.get('message')))
            print('</error>')
          print('</file>')
      print('</module>')
print('</results>')
