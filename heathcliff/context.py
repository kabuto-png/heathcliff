import os
import atexit
import tempfile

from pathlib import Path
from typing import Optional, Dict, Callable, Union
from moda.connect import Connector

class AppleSearchAdsCertificate(Connector):
  certificate: Dict[str, any]
  org_id: Optional[str]
  org_name: Optional[str]
  temp_dir: Optional[str]
  temp_certificate_dir: Optional[str]
  pem_path: Optional[str]
  key_path: Optional[str]
  clean_connection: Optional[Callable[[], None]]
  connection: Optional[Dict[str, str]] = None

  def __init__(self, certificate: Dict[str, any], temp_dir: Optional[str]=None):
    self.certificate = certificate
    self.org_id = self._get_certificate_property('org_id')
    self.org_name = self._get_certificate_property('org_name')
    self.temp_dir = temp_dir
    self.temp_certificate_dir = None
    self.pem_path = None
    self.key_path = None
    self.clean_connection = None
    self.connection = None

  def _get_certificate_property(self, suffix: str) -> Optional[str]:
    certificate_key = self._find_certificate_key(suffix=suffix)
    if certificate_key is None:
      return None
    value = self.certificate[certificate_key]
    if isinstance(value, bytes):
      return value.decode()
    elif isinstance(value, str):
      return value
    else:
      return None

  def _find_certificate_key(self, suffix: str) -> Optional[str]:
    filtered_keys = [f for f in self.certificate.keys() if not Path(f).name.startswith('.') and f.split('/')[-1].split('.')[-1] == suffix]
    if len(filtered_keys) != 1:
      return None
    return filtered_keys[0]

  def write_certificate_contents(self, contents: Union[bytes, str]) -> Path:
    os_temp_file, temp_file_path = tempfile.mkstemp(dir=self.temp_certificate_dir)
    os.close(os_temp_file)
    content_bytes = contents.encode() if isinstance(contents, str) else contents
    with open(temp_file_path, mode='wb') as temp_file:
      temp_file.write(content_bytes)
    return temp_file_path

  def connect(self):
    key_file = self._find_certificate_key('key')
    pem_file = self._find_certificate_key('pem')
    assert key_file and pem_file

    if key_file == 'key' and pem_file == 'pem':
      self.pem_path = self.certificate['pem']
      self.key_path = self.certificate['key']
      self.connection = {
        'pem': self.pem_path,
        'key': self.key_path,
      }
      return

    self.temp_certificate_dir = tempfile.mkdtemp(dir=self.temp_dir)
    self.pem_path = self.write_certificate_contents(contents=self.certificate[pem_file])
    self.key_path = self.write_certificate_contents(contents=self.certificate[key_file])

    key_path = Path(self.key_path)
    pem_path = Path(self.pem_path)
    temp_certificate_dir = Path(self.temp_certificate_dir)

    def cleanup_tempfiles():
      key_path.unlink()
      pem_path.unlink()
      temp_certificate_dir.rmdir()

    self.clean_connection = cleanup_tempfiles
    atexit.register(cleanup_tempfiles)
    self.connection = {
      'pem': pem_path,
      'key': key_path,
    }

  def disconnect(self):
    assert self.connection is not None
    if self.clean_connection is not None:
      self.clean_connection()
      atexit.unregister(self.clean_connection)
    self.temp_certificate_dir = None
    self.pem_path = None
    self.key_path = None
    self.clean_connection = None
    self.connection = None
