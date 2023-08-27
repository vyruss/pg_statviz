from pg_statviz.modules.buf import calc_buffers, calc_buff_rate
import psycopg2
from datetime import datetime,timedelta
import numpy 
static_time=datetime.now()
data= [
				{'buffers_checkpoint': 500, 'buffers_clean': 800, 'buffers_backend': 1200,'stats_reset': static_time,'snapshot_tstamp': datetime.now()+timedelta(seconds=1)},
				{'buffers_checkpoint': 300, 'buffers_clean': 600, 'buffers_backend': 900,'stats_reset': static_time,'snapshot_tstamp': datetime.now()+timedelta(seconds=2)},
				{'buffers_checkpoint': 1000, 'buffers_clean': 1500, 'buffers_backend': 2000,'stats_reset': static_time,'snapshot_tstamp': datetime.now()+timedelta(seconds=3)},
				{'buffers_checkpoint': 750, 'buffers_clean': 1100, 'buffers_backend': 1700,'stats_reset': static_time,'snapshot_tstamp': datetime.now()+timedelta(seconds=4)},
				{'buffers_checkpoint': 200, 'buffers_clean': 400, 'buffers_backend': 600,'stats_reset': static_time,'snapshot_tstamp': datetime.now()+timedelta(seconds=5)}
]

def test_buffer_calculations():

  response=calc_buffers(data,1024*1024*1024)
  
  total = [2500.0, 1800.0, 4500.0, 3550.0, 1200.0]
  checkpoints = [500.0, 300.0, 1000.0, 750.0, 200.0]
  bgwriter = [800.0, 600.0, 1500.0, 1100.0, 400.0]
  backends = [1200.0, 900.0, 2000.0, 1700.0, 600.0]
  
  assert total==response[0]
  assert checkpoints==response[1]
  assert bgwriter==response[2]
  assert backends==response[3]
  
def test_buffer_rate_calculations():
  response=calc_buff_rate(data,8192)
  
  total = [numpy.nan, -5.47, 21.1, -7.42, -18.36]
  checkpoints=[numpy.nan, -1.56, 5.5, -1.95, -4.3]
  bgwriter=[numpy.nan, -1.56, 7.0, -3.12, -5.47]
  backends=[numpy.nan, -2.34, 8.6, -2.34, -8.59]
  
  numpy.testing.assert_equal(numpy.array(total),numpy.array(response[0]))
  numpy.testing.assert_equal(numpy.array(checkpoints),numpy.array(response[1]))
  numpy.testing.assert_equal(numpy.array(bgwriter),numpy.array(response[2]))
  numpy.testing.assert_equal(numpy.array(backends),numpy.array(response[3]))

  
  
  
  
  
  