package dbresolver

import (
	"gorm.io/gorm"
)

type NamedConnPool struct {
	Name     string
	ConnPool gorm.ConnPool
}

type resolver struct {
	sources           []NamedConnPool
	replicas          []NamedConnPool
	policy            Policy
	dbResolver        *DBResolver
	traceResolverMode bool
}

func (r *resolver) sourcesConnPools() []gorm.ConnPool {
	cps := make([]gorm.ConnPool, 0, len(r.sources))
	for _, r := range r.sources {
		cps = append(cps, r.ConnPool)
	}
	return cps
}

func (r *resolver) replicaConnPools() []gorm.ConnPool {
	cps := make([]gorm.ConnPool, 0, len(r.replicas))
	for _, r := range r.replicas {
		cps = append(cps, r.ConnPool)
	}
	return cps
}

func (r *resolver) resolve(stmt *gorm.Statement, op Operation) (connPool gorm.ConnPool) {
	if op == Read {
		if len(r.replicas) == 1 {
			connPool = r.replicas[0].ConnPool
		} else {
			connPool = r.policy.Resolve(r.replicaConnPools())
		}
		if r.traceResolverMode {
			markStmtResolverMode(stmt, ResolverModeReplica)
		}
	} else if len(r.sources) == 1 {
		connPool = r.sourcesConnPools()[0]
		if r.traceResolverMode {
			markStmtResolverMode(stmt, ResolverModeSource)
		}
	} else {
		connPool = r.policy.Resolve(r.sourcesConnPools())
		if r.traceResolverMode {
			markStmtResolverMode(stmt, ResolverModeSource)
		}
	}

	if stmt.DB.PrepareStmt {
		if preparedStmt, ok := r.dbResolver.prepareStmtStore[connPool]; ok {
			return &gorm.PreparedStmtDB{
				ConnPool: connPool,
				Mux:      preparedStmt.Mux,
				Stmts:    preparedStmt.Stmts,
			}
		}
	}

	return
}

func (r *resolver) sourceCall(fc func(connPool gorm.ConnPool) error) error {
	for _, r := range r.sourcesConnPools() {
		if err := fc(r); err != nil {
			return err
		}
	}
	return nil
}

func (r *resolver) replicaCall(fc func(connPool gorm.ConnPool) error) error {
	for _, r := range r.replicaConnPools() {
		if err := fc(r); err != nil {
			return err
		}
	}
	return nil
}
