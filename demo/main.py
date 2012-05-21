#!/usr/bin/python

import urllib2, sys, re, web, json
from bs4 import BeautifulSoup  
from collections import defaultdict

if len(sys.argv) == 2:
  master = sys.argv[1]
  del sys.argv[1] # overriding 2nd arg; web.py expects an IP address here 
else: 
  master = 'http://ec2-23-20-149-114.compute-1.amazonaws.com'

master_url = master+':8080/'
dashboard_url = master+':8088/'

urls = ( 
  '/', 'index',
  '/status', 'status'
)

def main():
  app = web.application(urls, globals())
  app.run()        

class index:
  def GET(self):
    render = web.template.render('templates')
    return render.grid()

class status:
  def GET(self):
    return json.dumps({'slaves' : get_status()})

def get_status():
  total_tasks = 0
  total_cpus = 0
  total_nodes = 0
  total_mem_used = 0
  total_mem_cap = 0

  # Get slave list and basic info from master status page
  master_html = urllib2.urlopen(master_url).read() 
  master_soup = BeautifulSoup(master_html) 
  slave_rows = master_soup.find('h2', text='Slaves') \
    .find_next_sibling('table') \
    .find_all('tr')
  slaves = defaultdict(dict)
  for row in slave_rows:
    link = row.find('a')
    if link is not None:
      slave_keys = ['id', 'hostname', 'cpus', 'mem', 'connected']
      slave_values = map(lambda col: re.sub(r'\s*', r'', col.text), row.find_all('td'))
      slave_info = dict(zip(slave_keys, slave_values))
      slave_info['url'] = link['href']
      slave_name = get_slave_name(slave_info['hostname'])
      slaves[slave_name] = {'info' : slave_info, 'tasks' : 0}
      total_cpus += int(slave_info['cpus'])
      total_nodes += 1

  # Get tasks per slave
  active_frameworks = master_soup.find('h2', text='Active Frameworks') \
    .find_next_sibling('table')\
    .find_all('tr')

  if len(active_frameworks) > 0: 
    active_frameworks.pop(0)
  for framework in active_frameworks:
    framework_url = framework.find('a')['href']
    info_values = map(lambda x: (x.text).rstrip('\n\r'), framework.find_all('td'))
    info_keys = ['id', 'user', 'name', 'running', 'cpus', 'mem', 'share', 'connected']
    info = dict(zip(info_keys, info_values))

    # Get slave task info from framework status page
    if framework_url[0] == '/':
      framework_url = re.sub(r'^(\w+:\/+[^/]*)/.*', r'\1', master_url)+framework_url
    framework_html = urllib2.urlopen(framework_url).read()
    framework_soup = BeautifulSoup(framework_html)
    slave_links = framework_soup.find_all('a')

    # Get running tasts for each slave
    for link in slave_links: 
      slave = link.text
      table = link.find_parent('table')
      if table is not None:
        task_id = link.find_parent('tr').contents[1].text
        hostname = link.text
        if table.find_previous_sibling('h2').text == 'Running Tasks':
          slaves[get_slave_name(hostname)]['tasks'] += 1
          total_tasks += 1

  # Get memory usage per slave from Shark dashboard
  dashboard_html = urllib2.urlopen(dashboard_url).read() 
  dashboard_soup = BeautifulSoup(dashboard_html) 
  slave_rows = dashboard_soup.find_all('tr')
  for row in slave_rows:
    slave_values = map(lambda col: re.sub(r'\s*', r'', col.text), row.find_all('td', 'node'))
    if len(slave_values) > 0:
      slave_keys = ['hostname', 'max', 'cur', 'graph']
      slave_info = dict(zip(slave_keys, slave_values))
      slave_info['max'] = mem_str_to_mbs(slave_info['max'])
      slave_info['cur'] = mem_str_to_mbs(slave_info['cur'])
      slave_info['perc'] = int(round(100*float(slave_info['cur'])/float(slave_info['max'])))
      slave_name = get_slave_name(slave_info['hostname'])
      del slave_info['hostname']
      del slave_info['graph']
      slaves[slave_name]['mem'] = slave_info
      total_mem_cap += round(float(slave_info['max'])/1024, 1)
      total_mem_used += round(float(slave_info['cur'])/1024, 1)

  slaves['total_tasks'] = total_tasks
  slaves['total_cpus'] = total_cpus
  slaves['total_nodes'] = total_nodes
  slaves['total_mem_cap'] = total_mem_cap
  slaves['total_mem_used'] = total_mem_used
  return slaves

def get_slave_name(hostname):
  return re.sub(r'([^.])\..*', r'\1', hostname)

def mem_str_to_mbs(memstr):
  mem_size = float(memstr[0:len(memstr)-2])
  mem_unit = memstr[len(memstr)-2:len(memstr)]
  if mem_unit == 'GB':
    return int(mem_size*1024)
  else: 
    return int(mem_size)

if __name__ == '__main__': 
  main()

