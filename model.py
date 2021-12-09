# Database models
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, Float, String, DateTime, UnicodeText

Base = declarative_base()


class CISAWSReport(Base):
    """
    Report Database Model
    """
    __tablename__ = 'cis_aws_report'

    id = Column(Integer, primary_key=True, nullable=False, unique=True)

    # Datetime created
    dt = Column(DateTime, nullable=False)

    severity       = Column(String(20), nullable=False)
    status         = Column(String(20), nullable=False)
    setting_id     = Column(String(10), nullable=False)
    title          = Column(UnicodeText(), nullable=False)
    resources      = Column(UnicodeText(), nullable=False)
    benchmark      = Column(String(80), nullable=False)
    aws_account_id = Column(String(20), nullable=False)

    @classmethod
    def check_table_existence(cls, engine):
        """
        Check if target table exists, if not, created.
        :param engine: DB engine
        :return: N/A
        """
        cls.__table__.create(bind=engine, checkfirst=True)
        return True


class CISAWSReportStats(Base):
    """
    Report Statistic Database Model
    """
    __tablename__ = 'cis_aws_report_stats'

    id = Column(Integer, primary_key=True, nullable=False, unique=True)

    # Datetime created
    dt = Column(DateTime, nullable=False)

    # This is the number of all the settings. There will be duplicated settings
    # Because AWS is running the setting against all the eligible subjects
    total_settings      = Column(Integer, nullable=False)
    total_failed        = Column(Integer, nullable=False)
    total_not_available = Column(Integer, nullable=False)
    total_passed        = Column(Integer, nullable=False)

    # This will be the number of Benchmark settings for each report
    # There is a possibility that some reports may lose unique settings
    total_unique_settings      = Column(Integer, nullable=False)
    # total_unique_failed        = Column(Integer, nullable=False)
    # total_unique_not_available = Column(Integer, nullable=False)
    # total_unique_passed        = Column(Integer, nullable=False)

    # This core is calculated based on the total_unique numbers
    compliant_score = Column(Float, nullable=False)
    aws_account_id  = Column(String(20), nullable=False)
    benchmark       = Column(String(80), nullable=False)

    @classmethod
    def check_table_existence(cls, engine):
        """
        Check if target table exists, if not, created.
        :param engine: DB engine
        :return: N/A
        """
        cls.__table__.create(bind=engine, checkfirst=True)
        return True
