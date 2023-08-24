from pg_statviz.modules.buf import calc_buffers, calc_buff_rate

data= [
				{'buffers_checkpoint': 500, 'buffers_clean': 800, 'buffers_backend': 1200,'stats_reset': 0,'snapshot_tstamp': 1},
				{'buffers_checkpoint': 300, 'buffers_clean': 600, 'buffers_backend': 900,'stats_reset': 0,'snapshot_tstamp': 2},
				{'buffers_checkpoint': 1000, 'buffers_clean': 1500, 'buffers_backend': 2000,'stats_reset': 0,'snapshot_tstamp': 3},
				{'buffers_checkpoint': 750, 'buffers_clean': 1100, 'buffers_backend': 1700,'stats_reset': 0,'snapshot_tstamp': 4},
				{'buffers_checkpoint': 200, 'buffers_clean': 400, 'buffers_backend': 600,'stats_reset': 0,'snapshot_tstamp': 5}
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
  response=calc_buff_rate(data,1024*1024)
  # Requires mocking the database since the function uses methods provided by pycopg library (total_second())
  