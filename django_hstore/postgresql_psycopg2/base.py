import logging
import re
import sys
import traceback
from django import VERSION
from django.conf import settings
from django.db.backends.postgresql_psycopg2.base import *
from django.db.backends.util import truncate_name
from psycopg2.extras import register_hstore

from django_hstore.postgresql_psycopg2.operations import DatabaseOperations

log = logging.getLogger(__name__)
# Regexp for SQL comments
COMMENTS = re.compile(r'/\*.*?\*/', re.MULTILINE | re.DOTALL)
COMMENTS2 = re.compile(r'--.*?$', re.MULTILINE)


class DatabaseCreation(DatabaseCreation):
    def executescript(self, path, title='SQL'):
        """
        Load up a SQL script file and execute.
        """
        try:
            sql = ''.join(open(path).readlines())
            # strip out comments
            sql = COMMENTS.sub('',sql)
            sql = COMMENTS2.sub('',sql)
            # execute script line by line
            cursor = self.connection.cursor()
            self.set_autocommit()
            for l in re.split(r';', sql):
                l = l.strip()
                if len(l)>0:
                    try:
                        cursor.execute(l)
                    except Exception:
                        message = 'Error running % script: %s' % (title, l)
                        log.exception(message)
                        print >> sys.stderr, message
                        traceback.print_exc()
            log.info('Executed post setup for %s.', title)
        except Exception:
            message = 'Problem in %s script' % (title,)
            log.exception(message)
            print >> sys.stderr, message
            traceback.print_exc()

    def _create_test_db(self, verbosity, autoclobber):
        super(DatabaseCreation, self)._create_test_db(verbosity, autoclobber)
        register_hstore(self.connection.connection, globally=True, unicode=True)

    def sql_indexes_for_field(self, model, f, style):
        kwargs = VERSION[:2] >= (1, 3) and {'connection': self.connection} or {}
        if f.db_type(**kwargs) == 'hstore':
            if not f.db_index:
                return []
            # create GIST index for hstore column
            qn = self.connection.ops.quote_name
            index_name = '%s_%s_gist' % (model._meta.db_table, f.column)
            clauses = [style.SQL_KEYWORD('CREATE INDEX'),
                style.SQL_TABLE(qn(truncate_name(index_name, self.connection.ops.max_name_length()))),
                style.SQL_KEYWORD('ON'),
                style.SQL_TABLE(qn(model._meta.db_table)),
                style.SQL_KEYWORD('USING GIST'),
                '(%s)' % style.SQL_FIELD(qn(f.column))]
            # add tablespace clause
            tablespace = f.db_tablespace or model._meta.db_tablespace
            if tablespace:
                sql = self.connection.ops.tablespace_sql(tablespace)
                if sql:
                    clauses.append(sql)
            clauses.append(';')
            return [ ' '.join(clauses) ]
        return super(DatabaseCreation, self).sql_indexes_for_field(model, f, style)

    def sql_table_creation_suffix(self):
        try:
            template = settings.HSTORE_TEMPLATE
            return ' TEMPLATE %s' % self.connection.ops.quote_name(template)
        except AttributeError:
            return super(DatabaseCreation, self).sql_table_creation_suffix()


class DatabaseWrapper(DatabaseWrapper):
    """
    Custom DB wrapper to inject connection registration and DB creation code
    """

    def __init__(self, *args, **params):
        super(DatabaseWrapper, self).__init__(*args, **params)
        self.creation = DatabaseCreation(self)
        self.ops = DatabaseOperations(self)

    def _cursor(self):
        # ensure that we're connected
        cursor = super(DatabaseWrapper, self)._cursor()

        # register hstore extension
        register_hstore(self.connection, globally=True, unicode=True)

        # bypass future registrations
        self._cursor = super(DatabaseWrapper, self)._cursor
        return cursor
