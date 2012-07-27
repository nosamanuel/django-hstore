from django.db.backends.postgresql_psycopg2.operations import (
        DatabaseOperations as BaseDatabaseOperations)


class DatabaseOperations(BaseDatabaseOperations):
    def quote_name(self, name):
        super_qn = super(DatabaseOperations, self).quote_name
        if '->' in name:
            field, key = name.split('->')
            return '%s->%s' % (super_qn(field), key)
        else:
            return super_qn(name)
