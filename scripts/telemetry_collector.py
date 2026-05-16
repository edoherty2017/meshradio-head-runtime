#!/usr/bin/env python3
import json, os, re, time, datetime
import serial
PORT=os.environ.get('LORA_PORT','/dev/lora_radio')
BAUD=int(os.environ.get('LORA_BAUD','115200'))
TRIAL_ID=os.environ.get('TRIAL_ID','trial-unknown')
NODE_ID=os.environ.get('NODE_ID','meshradiohead')
HEAD_ID=os.environ.get('HEAD_ID','meshnodehead')
OUT_DIR=os.environ.get('OUT_DIR','/home/pump/telemetry_head')
raw_dir=os.path.join(OUT_DIR,'raw'); json_dir=os.path.join(OUT_DIR,'jsonl')
os.makedirs(raw_dir,exist_ok=True); os.makedirs(json_dir,exist_ok=True)
raw_path=os.path.join(raw_dir,'serial_raw.log'); json_path=os.path.join(json_dir,'telemetry_stream.jsonl')
ansi_re=re.compile(r'\x1b\[[0-9;]*m')
patterns={
 'battery_mv':re.compile(r'batMv\s*=\s*(\d+)',re.I),
 'battery_pct':re.compile(r'batPct\s*=\s*(\d+)',re.I),
 'usb_power':re.compile(r'(?:USB\s*power|usbPower)\s*=\s*(\d+)',re.I),
 'is_charging':re.compile(r'isCharging\s*=\s*(\d+)',re.I),
 'rssi_dbm':re.compile(r'RSSI\s*[:=]\s*(-?\d+)',re.I),
 'snr_db':re.compile(r'SNR\s*[:=]\s*(-?\d+(?:\.\d+)?)',re.I),
 'lat_lon':re.compile(r'lat\s*[:=]\s*(-?\d+\.\d+).{0,40}lon\s*[:=]\s*(-?\d+\.\d+)',re.I),
}
def ts(): return datetime.datetime.now(datetime.timezone.utc).isoformat()
while True:
  try:
    ser=serial.Serial(PORT,BAUD,timeout=0.5); ser.dtr=False; ser.rts=False
    with open(raw_path,'ab',buffering=0) as rawf, open(json_path,'a',encoding='utf-8') as jf:
      while True:
        b=ser.read(4096)
        if not b: continue
        rawf.write(b)
        for ln in b.decode('utf-8','ignore').splitlines():
          line=ansi_re.sub('',ln).strip()
          if not line: continue
          rec={'timestamp_utc':ts(),'trial_id':TRIAL_ID,'node_id':NODE_ID,'head_id':HEAD_ID,'line':line}
          for k in ('battery_mv','battery_pct','usb_power','is_charging','rssi_dbm','snr_db'):
            m=patterns[k].search(line)
            if m: rec[k]=float(m.group(1)) if k=='snr_db' else int(m.group(1))
          m=patterns['lat_lon'].search(line)
          if m: rec['lat']=float(m.group(1)); rec['lon']=float(m.group(2))
          jf.write(json.dumps(rec)+'\n'); jf.flush()
  except Exception as e:
    with open(os.path.join(OUT_DIR,'collector_errors.log'),'a',encoding='utf-8') as ef:
      ef.write(f"{ts()} collector_error {type(e).__name__}: {e}\n")
    time.sleep(2)
