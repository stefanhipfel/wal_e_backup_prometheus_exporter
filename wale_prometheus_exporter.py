#!/usr/bin/python
### Check if the backup wal-e postgres is executed successfully based on given BACKUP_SCHEDULE

import subprocess
import signal
import logging
from datetime import datetime, timedelta
import time
import os
from croniter import croniter
from prometheus_client.core import GaugeMetricFamily, REGISTRY
from prometheus_client import start_http_server

shutdown = False
logging.basicConfig(format='%(asctime)-15s %(process)d %(levelname)s %(filename)s:%(lineno)d %(message)s',
                        level=logging.DEBUG)
LOG = logging.getLogger(__name__)

class WaleMetricsCollector():
    def collect(self):
        self.time_metrics = GaugeMetricFamily("pg_cluster_wale_backup_time", "Shows the timestamp of the last successful postgres backup")
        self.success_metrics = GaugeMetricFamily("pg_cluster_wale_backup_success", "Shows if postgres backup is working as scheduled")
        self._getBackupList()
        yield self.time_metrics
        yield self.success_metrics

    def _getBackupList(self):
        command = "envdir $WALE_ENV_DIR wal-e backup-list | tail -1"

        try:
            schedule = os.environ["BACKUP_SCHEDULE"]
            #a backup takes ~ 15  secs to complete. So make sure we are not checking when a cron job is currently running, rather after it should be completed
            iter = croniter(schedule, datetime.now() - timedelta(seconds=15))
            last_expected = iter.get_prev(datetime)

            p = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True)
            output, err = p.communicate()
            last = output

            last_string = last.split()[1][:-5]
            last_string = str(last_string, "utf-8")
            last_datetime = datetime.strptime(last_string, "%Y-%m-%dT%H:%M:%S")
            timestamp = time.mktime(last_datetime.timetuple())

            self.time_metrics.add_metric([], value=timestamp)

            if last_datetime < last_expected:
                LOG.error("CRITICAL => Last backup: %s | Scheduled: %s" % last_datetime, last_expected )
                self.success_metrics.add_metric([], value=0)
            else:
                LOG.info("OK: %s" % last_datetime)
                self.success_metrics.add_metric([], value=1)
        except:
            self.success_metrics.add_metric([], value=0)
            pass


def _on_sigterm(signal, frame):
        global shutdown
        shutdown = True

signal.signal(signal.SIGINT, _on_sigterm)
signal.signal(signal.SIGTERM, _on_sigterm)

LOG.info("Starting wale prometheus server")
REGISTRY.register(WaleMetricsCollector())
start_http_server(9200, "0.0.0.0")

while not shutdown:
        time.sleep(1)

os._exit(0)
