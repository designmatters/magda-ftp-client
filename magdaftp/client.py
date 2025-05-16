import ftplib
from io import BytesIO
from pathlib import Path
from typing import List, Optional, Tuple, Union
from enum import Enum

FILE_PROCESSING_SUFFIX = '_processing'


class Folder(str, Enum):
    TO_VIP = "to_vip"
    FROM_VIP = "from_vip"
    CACHE_TO_VIP = "cache/to_vip"
    CACHE_FROM_VIP = "cache/from_vip"
    PUBLIEK = "publiek"


class FTP_TLS_With_Session_Reuse(ftplib.FTP_TLS):
    def ntransfercmd(self, cmd, rest=None):
        conn, size = ftplib.FTP.ntransfercmd(self, cmd, rest)
        if self._prot_p:
            conn = self.context.wrap_socket(
                conn,
                server_hostname=self.host,
                session=self.sock.session
            )
        return conn, size


class MagdaFtpClient:
    def __init__(
            self,
            server: str,
            username: str,
            certfile: Path,
            keyfile: Path,
            password: str = ''
    ):
        self.server = server
        self.username = username
        self.certfile = certfile
        self.keyfile = keyfile
        self.password = password

    def _connect(self) -> FTP_TLS_With_Session_Reuse:
        ftps = FTP_TLS_With_Session_Reuse(
            self.server,
            certfile=str(self.certfile),
            keyfile=str(self.keyfile)
        )
        ftps.login(self.username, self.password)
        ftps.prot_p()
        return ftps

    def list_files(self, folder: Union[Folder, str]) -> List[str]:
        folder_str = str(folder)
        with self._connect() as ftps:
            ftps.cwd(folder_str)
            return [
                f for f in ftps.nlst()
                if not f.endswith(FILE_PROCESSING_SUFFIX) and f not in ('.', '..')
            ]

    def get_file_content(
            self, folder: Union[Folder, str], filename: str, mark_as_processing: bool = True
    ) -> Tuple[str, str, str]:
        if not filename:
            return '', '', ''

        folder_str = str(folder)
        processing_name = filename

        with self._connect() as ftps:
            full_path = f'{folder_str}/{filename}'
            buffer = BytesIO()
            ftps.retrbinary(f'RETR {full_path}', buffer.write)
            content = buffer.getvalue().decode('utf-8')
            if mark_as_processing:
                processing_name += FILE_PROCESSING_SUFFIX
                ftps.rename(full_path, f'{folder_str}/{processing_name}')
        return processing_name, filename, content

    def upload_file(
            self, folder: Union[Folder, str], content: str, filename: str
    ) -> str:
        folder_str = str(folder)
        temp_name = f'{filename}.tmp'

        with self._connect() as ftps:
            ftps.cwd(folder_str)
            with BytesIO(content.encode()) as f:
                ftps.storbinary(f'STOR {temp_name}', f)
            ftps.rename(temp_name, filename)
        return filename

    def remove_file(self, folder: Union[Folder, str], filename: str):
        if not filename:
            return

        folder_str = str(folder)
        with self._connect() as ftps:
            ftps.delete(f'{folder_str}/{filename}')
