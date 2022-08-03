package hbase1

type HRecord interface {
	GetRowKey() []byte
	GetQualifiersMap() map[string][]byte
}
