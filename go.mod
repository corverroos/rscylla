module github.com/corverroos/rscylla

go 1.15

require (
	github.com/gocql/gocql v0.0.0-20201215165327-e49edf966d90
	github.com/luno/jettison v0.0.0-20210113153543-156bb7eff2dc
	github.com/luno/reflex v0.0.0-20210120094531-14dacdfab1a9
	github.com/scylladb/scylla-cdc-go v0.0.0-20210204170501-229d928c650e
	github.com/stretchr/testify v1.6.0
	golang.org/x/sync v0.0.0-20200317015054-43a5402ce75a
)

replace github.com/gocql/gocql => github.com/scylladb/gocql v1.4.3
