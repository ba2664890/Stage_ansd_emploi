import psycopg2
import hashlib
import re
from datetime import datetime
import scrapy
from scrapy.exceptions import DropItem
from psycopg2.extras import execute_values


def clean_list(value):
    if isinstance(value, list):
        return str(value[0]).strip() if value else None
    return str(value).strip() if value else None

def clean_int(value):
    try:
        return int(re.sub(r'\D', '', str(value).strip()))
    except (ValueError, AttributeError):
        return None

def clean_float(value):
    try:
        return float(re.search(r'(\d+(?:\.\d+)?)', str(value).replace('\u202f', '').replace(' ', '')).group(1))
    except (ValueError, AttributeError):
        return None

def process_item(self, item, spider):
    # on flush à chaque fois
    self.buffer.append({k: item.get(k) for k in self.fields})
    self._flush()
    return item

class DuplicatesPipeline:
    def __init__(self):
        self.urls_seen = set()

    def process_item(self, item, spider):
        url_raw = item["url"][0] if isinstance(item["url"], list) else item["url"]
        url_hash = hashlib.md5(url_raw.encode()).hexdigest()
        if url_hash in self.urls_seen:
            raise DropItem(f"URL déjà traitée : {url_raw}")
        self.urls_seen.add(url_hash)
        # ➜ on ne touche pas à item, on renvoie juste l’item tel quel
        return item
    
class DuplicatesPipeline:
    def __init__(self):
        self.urls_seen = set()

    def process_item(self, item, spider):
        url_raw = item["url"][0] if isinstance(item["url"], list) else item["url"]
        url_hash = hashlib.md5(url_raw.encode()).hexdigest()
        if url_hash in self.urls_seen:
            raise DropItem(f"URL déjà traitée : {url_raw}")
        self.urls_seen.add(url_hash)
        item["id"] = url_hash      # ← on remet cette ligne
        return item

# ------------------------------------------------------------------
# Pipeline JOBS – emploisenegal.com
# ------------------------------------------------------------------
from .model import Emploi
class EmploiSenegalPostgreSQLPipeline:
    def __init__(self, database, user, password, host, port):
        self.db_params = dict(database=database, user=user, password=password, host=host, port=port)
        self.batch_size = 1          # ← flush immédiat
        self.buffer = []
        self.fields = ["id", "url", "title", "company_name", "company_sectors", "description",
                       "contract", "region", "education", "experience",
                       "skills", "posted", "source", "scraped_at", "metier_type", "metiers", "job_missions", "job_profile", "job_criteria", "job_skills", "job_count"]
    # ---------- vie du spider ----------
    def open_spider(self, spider):
        self.conn = psycopg2.connect(**self.db_params)
        self._ensure_table()
        spider.logger.info("[PG-REALTIME] connecté → %s", self.db_params["database"])

    def close_spider(self, spider):
        if self.buffer:
            self._flush(spider)

    def process_item(self, item, spider):
        # on met dans le buffer et on flush tout de suite
        self.buffer.append({k: item.get(k) for k in self.fields})
        self._flush(spider)
        return item

    @classmethod
    def from_crawler(cls, crawler):
        db = crawler.settings.getdict("DATABASE")
        return cls(
            database=db["database"],
            user=db["user"],
            password=db["password"],
            host=db["host"],
            port=db["port"],
        )
    # ---------- SQL ----------
    def _ensure_table(self):
        with self.conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS jobs_emploi_senegal(
                    id VARCHAR(32) PRIMARY KEY, url TEXT UNIQUE, title TEXT,
                    company_name TEXT, company_sectors TEXT, description TEXT, contract TEXT, region TEXT,
                    education TEXT, experience TEXT, skills TEXT, posted DATE, metiers TEXT, metier_type TEXT,
                    source TEXT, scraped_at TIMESTAMP DEFAULT NOW(),
                    job_missions TEXT, job_profile TEXT, job_criteria TEXT, job_skills TEXT, job_count TEXT
                );
            """)
            self.conn.commit()

    def _flush(self, spider):
        if not self.buffer:
            return
        # complétion clés manquantes
        required = {"id", "url", "title", "company", "description",
                    "contract", "region", "education", "experience",
                    "skills", "posted", "source", "scraped_at"}
        for row in self.buffer:
            for k in required:
                row.setdefault(k, None)

        with self.conn.cursor() as cur:
            for row in self.buffer:
                cur.execute("""
                    INSERT INTO jobs_emploi_senegal(
                        id, url, title, company_name, company_sectors, metier_type, metiers, description,
                        contract, region, education, experience,
                        skills, posted, source, scraped_at, job_missions, job_profile, job_criteria, job_skills, job_count
                    )
                    VALUES (
                        %(id)s, %(url)s, %(title)s, %(company_name)s, %(company_sectors)s, %(metier_type)s, %(metiers)s,
                        %(description)s, %(contract)s, %(region)s,
                        %(education)s, %(experience)s, %(skills)s,
                        %(posted)s, %(source)s, %(scraped_at)s, %(job_missions)s, %(job_profile)s, %(job_criteria)s, %(job_skills)s, %(job_count)s
                    )
                    ON CONFLICT (url) DO UPDATE
                    SET title       = EXCLUDED.title,
                        company_name = EXCLUDED.company_name,
                        company_sectors = EXCLUDED.company_sectors,
                        description = EXCLUDED.description,
                        contract    = EXCLUDED.contract,
                        region      = EXCLUDED.region,
                        education   = EXCLUDED.education,
                        experience  = EXCLUDED.experience,
                        skills      = EXCLUDED.skills,
                        posted      = EXCLUDED.posted,
                        scraped_at  = EXCLUDED.scraped_at;
                """, row)
                # ---------- LOG TEMPS REEL ----------
                spider.logger.info("[PG-INSERT] %s", row["url"])
            self.conn.commit()
        self.buffer.clear()





# ------------------------------------------------------------------
# Pipeline JOBS – emploiedakar.com
# ------------------------------------------------------------------


from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from .model import Base, Emploi
from datetime import datetime


class SQLAlchemyPipeline:
    def __init__(self):
        # Met à jour ton URI PostgreSQL avec user/password corrects
        self.engine = create_engine(
            "postgresql+psycopg2://Cardan:Fatimata05?@localhost:5432/scrapy_immo",
            pool_pre_ping=True
        )
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()

    def process_item(self, item, spider):
        # description
        if isinstance(item.get("description"), list):
            item["description"] = "\n".join([d.strip() for d in item["description"] if d.strip()])

        # location
        if isinstance(item.get("location"), list):
            item["location"] = ", ".join([l.strip() for l in item["location"] if l.strip()])

        # posted_date
        if isinstance(item.get("posted_date"), list) and item.get("posted_date"):
            date_str = item["posted_date"][0].strip()
            try:
                item["posted_date"] = datetime.strptime(date_str, "%Y-%m-%d").date()  # adapter le format réel
            except ValueError:
                item["posted_date"] = None
        elif isinstance(item.get("posted_date"), str):
            try:
                item["posted_date"] = datetime.strptime(item["posted_date"], "%Y-%m-%d").date()
            except ValueError:
                item["posted_date"] = None
        else:
            item["posted_date"] = None


        # title, url, source
        for field in ["title", "url", "source", "company_name", "location", "contract_type"]:
            val = item.get(field)
            if isinstance(val, list):
                item[field] = val[0]  # prend le premier élément


        emploi = Emploi(
            id=item["id"],
            title=item.get("title"),        # string
            url=item.get("url"),            # string
            location=item.get("location"),  # string
            posted_date=item.get("posted_date"),  # date
            source=item.get("source"),      # string
            description_p=item.get("description_p"),  # string
            description_ul=item.get("description_ul"),  # string
            company_name=item.get("company_name"),  # string
            contract_type=item.get("contract_type")  # string

        )


        try:
            self.session.merge(emploi)
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            spider.logger.error(f"Erreur insertion : {e}")

        return item


# ------------------------------------------------------------------
# Pipeline JOBS – senjob.com
# ------------------------------------------------------------------

from .model import senjob

class senjobPipeline():
    def __init__(self):
        # Met à jour ton URI PostgreSQL avec user/password corrects
        self.engine = create_engine(
            "postgresql+psycopg2://Cardan:Fatimata05?@localhost:5432/scrapy_immo",
            pool_pre_ping=True
        )
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()

    def process_item(self, item, spider):
        # description
        if isinstance(item.get("description"), list):
            item["description"] = "\n".join([d.strip() for d in item["description"] if d.strip()])

        # location
        if isinstance(item.get("location"), list):
            item["location"] = ", ".join([l.strip() for l in item["location"] if l.strip()])


        # title, url, source
        for field in ["title", "url", "source", "company_name"]:
            val = item.get(field)
            if isinstance(val, list):
                item[field] = val[0]  # prend le premier élément


        emploi = senjob(
            id=item["id"],
            title=item.get("title"),        # string
            url=item.get("url"),            # string
            location=item.get("location"),  # string
            posted_date=item.get("posted_date"),  # date
            categorie=item.get("categorie"),  # string
            source=item.get("source"),      # string
            description=item.get("description"),  # string
            salaire=item.get("salaire"),  # string
            expiration=item.get("expiration"),  # string
            contract_type=item.get("contract_type")  # string

        )


        try:
            self.session.merge(emploi)
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            spider.logger.error(f"Erreur insertion : {e}")

        return item


# ------------------------------------------------------------------
# Pipeline JOBS – emploi_expatdakar.com
# ------------------------------------------------------------------
# Pipeline pour expat-dakar
from .model import emploi_expatDakar
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

class ExpatDakarPipeline:
    def __init__(self):
        self.engine = create_engine("postgresql+psycopg2://Cardan:Fatimata05?@localhost:5432/scrapy_immo", pool_pre_ping=True)
        self.Session = sessionmaker(bind=self.engine)
        Base.metadata.create_all(self.engine)
    def open_spider(self, spider):
        self.session = self.Session()
    
    def close_spider(self, spider):
        self.session.close()

    # enleve les formats listes
    

    def process_item(self, item, spider):
        import re
        def clean(val):
            if isinstance(val, list):
                val = val[0]
            if isinstance(val, str):
                val = val.strip()
                val = re.sub(r"\s+", " ", val)  # supprime espaces multiples
                val = re.sub(r"[\r\n\t]", "", val)  # supprime retours ligne/tab
                val = re.sub(r"[^\w\sÀ-ÿ&'-]", "", val)  # supprime caractères spéciaux sauf lettres, chiffres, espace, accentués, &, ', -
            return val
        for field in ["title", "url", "source", "posted_date", "location","region", "description", "type_contrat", "employeur", "secteur", "niveau", "niveau_etude", "experience", "nb_postes"]:
            if field != "description":  # description peut être long, on ne la nettoie pas trop
                item[field] = clean(item.get(field))
            else:
                val = item.get(field)
                if isinstance(val, list):
                    val = val[0]
                item[field] = val

        expat = emploi_expatDakar(
            id=item.get('id'),
            title=item.get('title'),
            posted_date=item.get('posted_date'),
            ad_id=item.get('ad_id'),
            location=item.get('location'),
            region=item.get('region'),
            employeur=item.get('employeur'),
            secteur=item.get('secteur'),
            type_contrat=item.get('type_contrat'),
            niveau=item.get('niveau'),
            niveau_etude=item.get('niveau_etude'),
            experience=item.get('experience'),
            nb_postes=item.get('nb_postes'),
            description=item.get('description'),
            url=item.get('url'),
            source=item.get('source'),
        )
        try:
            self.session.merge(expat)
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            spider.logger.error(f"Erreur insertion expat-dakar : {e}")
        return item