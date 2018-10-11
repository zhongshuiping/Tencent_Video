from bo_lib.general.mongodb_helper import MongoDBHelper
from bo_lib.general.slack_notifier import BONotifier

class Reduce_coll(object):
    def __init__(self):
        self.client = MongoDBHelper()
        self.video_list_coll = self.client.get_collection(collection_name='video_list', database_name='TX_Video')
        self.cid_vid_coll = self.client.get_collection(collection_name='cid_vid', database_name='TX_Video')

    def reduce_decument(self):
        cid_vid_cursor = self.cid_vid_coll.find({'vids': {'$exists': 1}},
                                                {'_id': 0, 'cid': 1, 'vids': 1})
        cid_vid_list = [item for item in cid_vid_cursor]
        cid_vid_cursor.close()
        self.target_cids = self._get_dup_cid(cid_vid_list)
        try:BONotifier().msg('TX_Video reduce_decument: 所有栏目list=={}'.format(len(self.target_cids)), '@kang')
        except:pass
        self.video_list_coll.update({'cid': {'$in': list(self.target_cids)}},
                                     {'$set': {'dup_flag': 1}}, upsert=False, multi=True)
        self.cid_vid_coll.remove({'cid': {'$in': list(self.target_cids)}}, multi=True)

    def _get_dup_cid(self, cid_vid_list):
        counter_dict = dict()
        for vids_dict in cid_vid_list:
            cid = vids_dict['cid']
            vids_list = vids_dict['vids']
            for vid in vids_list:
                counter_dict.setdefault(vid, {})
                vid_dict = counter_dict[vid]
                vid_dict.setdefault('count', 0)
                vid_dict.setdefault('cid_vid_num', [])
                vid_dict['count'] += 1
                vid_dict['cid_vid_num'].append((cid, len(vids_list)))
        dup_vid_cid_list = list()
        dup_cid_set = set()
        for k, v in counter_dict.items():
            if v['count'] > 1:
                dup_vid_cid_list.append(v['cid_vid_num'])
        for each in dup_vid_cid_list:
            each.sort(key=lambda x: x[1])
            for cid_tup in each[:-1]:
                dup_cid_set.add(cid_tup[0])
        return dup_cid_set

if __name__ == '__main__':
    Reduce_coll().reduce_decument()
