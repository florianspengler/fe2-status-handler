import imaplib
import smtplib
from smtplib import SMTP, SMTP_SSL
import email
import time
import re
import json
import ssl
import logging
import os
import socket
from dataclasses import dataclass, field
from typing import Optional, Tuple, List
from email.message import EmailMessage
from email.header import decode_header, make_header
from email.policy import default as default_policy
from pathlib import Path

# ================== DATAKLASSEN / CONFIG ==================

@dataclass
class IMAPCfg:
    host: str
    user: str
    pass_: str
    folder: str = "INBOX"

@dataclass
class SMTPCfg:
    host: str
    port: int = 465
    security: str = "ssl"           # "ssl" (implicit TLS/SMTPS) oder "starttls"
    user: Optional[str] = None
    pass_: Optional[str] = None
    from_: Optional[str] = None     # Default: user
    to: str = ""
    subject: str = "Status-Update"
    body: str = "Hallo,\nim Anhang finden Sie den Status.\n"
    timeout_seconds: int = 30
    debug: bool = False

@dataclass
class BehaviorCfg:
    poll_interval_seconds: int = 15
    move_processed_to: str = ""     # leer = nicht verschieben
    max_messages_per_cycle: int = 200

@dataclass
class ParsingCfg:
    require_prefix: str = "2FMS"
    pattern: str = r"DATA>id:([^,<>]+),\s*status:([0-9]+)<DATA"

@dataclass
class LoggingCfg:
    level: str = "INFO"

@dataclass
class AppCfg:
    imap: IMAPCfg
    smtp: SMTPCfg
    behavior: BehaviorCfg = field(default_factory=BehaviorCfg)
    parsing: ParsingCfg = field(default_factory=ParsingCfg)
    logging: LoggingCfg = field(default_factory=LoggingCfg)

# ================== CONFIG-LOADER ==================

class ConfigLoader:
    def __init__(self, path: Path):
        self.path = path
        self._mtime: Optional[float] = None
        self.cfg: Optional[AppCfg] = None

    def load(self) -> AppCfg:
        if not self.path.exists():
            raise FileNotFoundError(f"config.json nicht gefunden: {self.path}")
        raw = json.loads(self.path.read_text(encoding="utf-8"))
        cfg = self._parse_raw(raw)
        self._mtime = self.path.stat().st_mtime
        self.cfg = cfg
        return cfg

    def maybe_reload(self) -> Optional[AppCfg]:
        try:
            mtime = self.path.stat().st_mtime
        except FileNotFoundError:
            logging.error("config.json fehlt: %s", self.path)
            return None
        if self._mtime is None or mtime > self._mtime:
            logging.info("Änderung an config.json erkannt – lade neu …")
            return self.load()
        return None

    def _parse_raw(self, raw: dict) -> AppCfg:
        try:
            imap_raw = raw.get("imap", {})
            smtp_raw = raw.get("smtp", {})
            beh_raw  = raw.get("behavior", {})
            par_raw  = raw.get("parsing", {})
            log_raw  = raw.get("logging", {})

            imap = IMAPCfg(
                host=imap_raw["host"],
                user=imap_raw["user"],
                pass_=imap_raw.get("pass") or imap_raw.get("password") or "",
                folder=imap_raw.get("folder", "INBOX"),
            )
            if not imap.pass_:
                raise ValueError("imap.pass (Passwort) fehlt")

            smtp_user = smtp_raw.get("user")
            smtp_from = smtp_raw.get("from") or smtp_user

            smtp = SMTPCfg(
                host=smtp_raw["host"],
                port=int(smtp_raw.get("port", 465)),
                security=str(smtp_raw.get("security", "ssl")).lower(),
                user=smtp_user,
                pass_=smtp_raw.get("pass") or smtp_raw.get("password"),
                from_=smtp_from,
                to=smtp_raw["to"],
                subject=smtp_raw.get("subject", "Status-Update"),
                body=smtp_raw.get("body", "Hallo,\nim Anhang finden Sie den Status.\n"),
                timeout_seconds=int(smtp_raw.get("timeout_seconds", 30)),
                debug=bool(smtp_raw.get("debug", False)),
            )
            if smtp.user and not smtp.pass_:
                raise ValueError("smtp.pass (Passwort) fehlt, obwohl smtp.user gesetzt ist")
            if not smtp.from_:
                raise ValueError("smtp.from fehlt (oder smtp.user setzen)")

            behavior = BehaviorCfg(
                poll_interval_seconds=int(beh_raw.get("poll_interval_seconds", 15)),
                move_processed_to=beh_raw.get("move_processed_to", ""),
                max_messages_per_cycle=int(beh_raw.get("max_messages_per_cycle", 200)),
            )
            parsing = ParsingCfg(
                require_prefix=par_raw.get("require_prefix", "2FMS"),
                pattern=par_raw.get("pattern", r"DATA>id:([^,<>]+),\s*status:([0-9]+)<DATA"),
            )
            logging_cfg = LoggingCfg(level=str(log_raw.get("level", "INFO")).upper())

            return AppCfg(imap=imap, smtp=smtp, behavior=behavior, parsing=parsing, logging=logging_cfg)
        except KeyError as e:
            raise ValueError(f"Pflichtfeld in config.json fehlt: {e}")

# ================== LOGGING ==================

def init_logging(level: str):
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

# ================== IMAP-HILFSFUNKTIONEN ==================

def connect_imap(cfg: AppCfg):
    logging.info("Verbinde zu IMAP: %s", cfg.imap.host)
    M = imaplib.IMAP4_SSL(cfg.imap.host)
    M.login(cfg.imap.user, cfg.imap.pass_)
    status, _ = M.select(f'"{cfg.imap.folder}"', readonly=False)
    if status != "OK":
        raise RuntimeError(f"Ordner {cfg.imap.folder} konnte nicht geöffnet werden.")
    return M

def search_unseen_uids(M) -> List[bytes]:
    status, data = M.search(None, 'UNSEEN')
    if status != "OK":
        return []
    ids = data[0].split()
    return ids

def fetch_message(M, uid):
    status, data = M.fetch(uid, '(RFC822)')
    if status != "OK" or not data or data[0] is None:
        return None
    raw = data[0][1]
    return email.message_from_bytes(raw, policy=default_policy)

def extract_text_from_email(msg) -> str:
    parts = []
    if msg.is_multipart():
        for part in msg.walk():
            ctype = part.get_content_type()
            if ctype == "text/plain":
                parts.append(part.get_content())
        if not parts:
            for part in msg.walk():
                ctype = part.get_content_type()
                if ctype == "text/html":
                    parts.append(part.get_content())
    else:
        parts.append(msg.get_content())
    return "\n\n".join([p if isinstance(p, str) else str(p) for p in parts])

# ================== PARSING ==================

def compile_patterns(cfg: AppCfg):
    re_prefix = re.compile(re.escape(cfg.parsing.require_prefix), re.IGNORECASE | re.DOTALL) if cfg.parsing.require_prefix else None
    re_data = re.compile(cfg.parsing.pattern, re.IGNORECASE)
    return re_prefix, re_data

def parse_fms_payload(text: str, re_prefix, re_data) -> Optional[Tuple[str, str]]:
    if re_prefix and not re_prefix.search(text):
        return None
    m = re_data.search(text)
    if not m:
        return None
    id_val = m.group(1).strip()
    status_val = m.group(2).strip()
    return id_val, status_val

# ================== SMTP / VERSAND ==================

def build_status_json(address: str, status_val: str) -> str:
    payload = {
        "type": "STATUS",
        "address": address,
        "data": {"status": str(status_val)}
    }
    return json.dumps(payload, ensure_ascii=False, indent=2)

def _send_via_ssl(cfg: SMTPCfg, msg: EmailMessage, context: ssl.SSLContext):
    with SMTP_SSL(cfg.host, cfg.port, timeout=cfg.timeout_seconds, context=context) as s:
        if cfg.debug:
            s.set_debuglevel(1)
        if cfg.user:
            s.login(cfg.user, cfg.pass_ or "")
        s.send_message(msg)

def _send_via_starttls(cfg: SMTPCfg, msg: EmailMessage, context: ssl.SSLContext):
    with SMTP(cfg.host, cfg.port, timeout=cfg.timeout_seconds) as s:
        if cfg.debug:
            s.set_debuglevel(1)
        s.ehlo()
        s.starttls(context=context)
        s.ehlo()
        if cfg.user:
            s.login(cfg.user, cfg.pass_ or "")
        s.send_message(msg)

def send_status_mail(cfg: AppCfg, address: str, status_val: str, original_msg=None):
    json_text = build_status_json(address, status_val)

    msg = EmailMessage()
    msg["From"] = cfg.smtp.from_ or cfg.smtp.user or ""
    msg["To"] = cfg.smtp.to
    msg["Subject"] = cfg.smtp.subject
    msg.set_content(cfg.smtp.body)

    msg.add_attachment(
        json_text.encode("utf-8"),
        maintype="application",
        subtype="json",
        filename="status.json"
    )

    if original_msg:
        try:
            orig_subject = str(make_header(decode_header(original_msg.get("Subject", ""))))
            msg.add_header("X-Source-Subject", orig_subject)
        except Exception:
            pass

    logging.info(
        "Sende Status-Mail an %s (address=%s, status=%s, %s:%s %s)",
        cfg.smtp.to, address, status_val, cfg.smtp.host, cfg.smtp.port, cfg.smtp.security
    )

    context = ssl.create_default_context()

    try:
        if cfg.smtp.security == "ssl":
            _send_via_ssl(cfg.smtp, msg, context)
        elif cfg.smtp.security == "starttls":
            _send_via_starttls(cfg.smtp, msg, context)
        else:
            # UNSICHER – nur falls bewusst so konfiguriert
            with SMTP(cfg.smtp.host, cfg.smtp.port, timeout=cfg.smtp.timeout_seconds) as s:
                if cfg.smtp.debug:
                    s.set_debuglevel(1)
                if cfg.smtp.user:
                    s.login(cfg.smtp.user, cfg.smtp.pass_ or "")
                s.send_message(msg)

    except (smtplib.SMTPServerDisconnected, socket.timeout) as e:
        logging.error("SMTP-Verbindung getrennt/Timeout: %s", e)
        # Fallback: wenn STARTTLS/587 fehlschlägt, versuche SSL/465
        if cfg.smtp.security == "starttls" and cfg.smtp.port == 587:
            logging.info("Fallback auf SMTPS (465/ssl) wird versucht …")
            fallback = SMTPCfg(
                host=cfg.smtp.host, port=465, security="ssl",
                user=cfg.smtp.user, pass_=cfg.smtp.pass_, from_=cfg.smtp.from_,
                to=cfg.smtp.to, subject=cfg.smtp.subject, body=cfg.smtp.body,
                timeout_seconds=cfg.smtp.timeout_seconds, debug=cfg.smtp.debug
            )
            _send_via_ssl(fallback, msg, context)
        else:
            raise

# ================== NACHRICHTENFLUSS ==================

def move_or_flag_processed(M, uid, cfg: AppCfg):
    M.store(uid, '+FLAGS', '\\Seen')
    if cfg.behavior.move_processed_to:
        try:
            M.copy(uid, cfg.behavior.move_processed_to)
            M.store(uid, '+FLAGS', '\\Deleted')
            M.expunge()
            logging.info("Mail %s nach '%s' verschoben.", uid.decode(), cfg.behavior.move_processed_to)
        except Exception as e:
            logging.warning("Verschieben fehlgeschlagen (%s). Flagge nur als gesehen.", e)

# ================== MAIN-LOOP ==================

def main():
    # Pfad zur config.json:
    script_dir = Path(__file__).resolve().parent
    cfg_path = Path(os.getenv("CONFIG_PATH", script_dir / "config.json"))
    loader = ConfigLoader(cfg_path)
    # Minimal-Logging bis Config geladen ist:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    cfg = loader.load()
    init_logging(cfg.logging.level)
    logging.info("Konfiguration geladen: %s", cfg_path)

    re_prefix, re_data = compile_patterns(cfg)

    while True:
        try:
            # Hot-Reload der Config
            new_cfg = loader.maybe_reload()
            if new_cfg:
                cfg = new_cfg
                logging.getLogger().setLevel(getattr(logging, cfg.logging.level, logging.INFO))
                re_prefix, re_data = compile_patterns(cfg)
                logging.info("Konfiguration neu geladen.")

            M = connect_imap(cfg)
            uids = search_unseen_uids(M)
            if uids:
                logging.info("Gefundene ungelesene Nachrichten: %d", len(uids))

            # ggf. deckeln, damit sehr volle Postfächer nicht alles auf einmal triggern
            if len(uids) > cfg.behavior.max_messages_per_cycle:
                uids = uids[:cfg.behavior.max_messages_per_cycle]

            for uid in uids:
                try:
                    msg = fetch_message(M, uid)
                    if not msg:
                        continue
                    body_text = extract_text_from_email(msg)
                    parsed = parse_fms_payload(body_text, re_prefix, re_data)
                    if parsed:
                        address, status_val = parsed
                        send_status_mail(cfg, address, status_val, original_msg=msg)
                        move_or_flag_processed(M, uid, cfg)
                except Exception as e:
                    logging.exception("Fehler bei Verarbeitung UID %s: %s", uid, e)
            try:
                M.logout()
            except Exception:
                pass

        except Exception as e:
            logging.exception("IMAP-Zyklusfehler: %s", e)

        time.sleep(cfg.behavior.poll_interval_seconds)

if __name__ == "__main__":
    main()