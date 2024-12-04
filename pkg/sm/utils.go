package sm

import (
	"adaptodb/pkg/schema"
)

func (s *KVStore) updateSchedule(flag string, krs []schema.KeyRange) uint64 {
	numKeysModified := 0
	if flag == "remove" {
		// remove krs from existing krs
		s.krs = schema.RemoveKeyRanges(s.krs, krs)
		s.Data.Range(func(key, value interface{}) bool {
			k := key.(string)
			for _, kr := range krs {
				if k >= kr.Start && k < kr.End {
					s.Data.Delete(k)
					s.NumEntries--
					numKeysModified++
					break
				}
			}
			return true
		})
	} else if flag == "add" {
		// add krs to existing krs
		s.krs = schema.AddKeyRanges(s.krs, krs)
		s.Data.Range(func(key, value interface{}) bool {
			k := key.(string)
			for _, kr := range krs {
				if k >= kr.Start && k < kr.End {
					numKeysModified++
					// Do not call s.NumEntries++ here, as it has already been incremented in Update()
					break
				}
			}
			return true
		})
	} else {
		// force apply to krs
		s.krs = krs
		s.Data.Range(func(key, value interface{}) bool {
			k := key.(string)
			flag := false
			for _, kr := range krs {
				if k >= kr.Start && k < kr.End {
					flag = true
					break
				}
			}
			if !flag {
				s.Data.Delete(k)
				s.NumEntries--
				numKeysModified++
			}
			return true
		})
	}
	return uint64(numKeysModified)
}
