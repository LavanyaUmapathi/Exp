import mock
import dags
from dags.common.safepass import get_safepass, cache


def test_safepass():
    credentials = [{
        u'credential': {
            u'category': u'sql_server',
            u'created_at': u'2015-09-12T12:05:11.000-',
            u'description': u'SA3_EBI_ETL',
            u'hostname': u'e@ebi-prod-01.corp:1433',
            u'id': 70225,
            u'password': u'reds',
            u'prerequisites': None,
            u'project_id': 9004,
            u'updated_at': u'2015-09-12T12:05:11.000',
            u'url': u'',
            u'user_id': 13737,
            u'username': u'ad',
            u'version': 1
        }
    }, {
        u'credential': {
            u'category': u'sql_server',
            u'created_at': u'2015-09-12T12:05:11.000-',
            u'description': u'RT_CLOUD',
            u'hostname': u'clo@100.200.300.48:3306',
            u'id': 70227,
            u'password': u'c***CE',
            u'prerequisites': None,
            u'project_id': 9004,
            u'updated_at': u'2015-09-12T12:05:11.:00',
            u'url': u'',
            u'user_id': 13737,
            u'username': u'udops',
            u'version': 1
        }
    }]

    with mock.patch.object(dags.common.rspwsafe.PWSafe, 'get_creds')\
            as get_creds:
        get_creds.return_value = credentials
        output = get_safepass(9004, 70225)

        assert output is not None
        assert output[0] == 'e@ebi-prod-01.corp:1433'
        assert output[1] == 'ad'
        assert output[2] == 'reds'

        output = get_safepass(9004, 111111)
        assert output is None
        cache.purge()


#Caching disable due to not implemented error handling
# def test_caching():
#     with mock.patch.object(dags.common.rspwsafe.PWSafe, 'get_creds')\
#             as get_creds:
#         get_creds.return_value = []
#         get_safepass(9004, 70225)
#         get_creds.assert_called_once_with()

#         get_safepass(9004, 70225)
#         get_creds.assert_called_once_with()
#         cache.purge()
