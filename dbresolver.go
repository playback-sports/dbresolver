package dbresolver

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"sync"
	"time"

	"gorm.io/gorm"
)

const (
	Write Operation = "write"
	Read  Operation = "read"
)

type DBResolver struct {
	*gorm.DB
	configs          []Config
	resolvers        map[string]*resolver
	global           *resolver
	prepareStmtStore map[gorm.ConnPool]*gorm.PreparedStmtDB
	compileCallbacks []func(gorm.ConnPool) error
}

type NamedDialectors []NamedDialector

type NamedDialector struct {
	Name      string
	Dialector gorm.Dialector
}

func (nd NamedDialectors) Dialectors() (dialectors []gorm.Dialector) {
	for _, d := range nd {
		dialectors = append(dialectors, d.Dialector)
	}
	return dialectors
}

type Config struct {
	Sources           NamedDialectors
	Replicas          NamedDialectors
	Policy            Policy
	datas             []interface{}
	TraceResolverMode bool
}

func Register(config Config, datas ...interface{}) *DBResolver {
	return (&DBResolver{}).Register(config, datas...)
}

func (dr *DBResolver) Register(config Config, datas ...interface{}) *DBResolver {
	if dr.prepareStmtStore == nil {
		dr.prepareStmtStore = map[gorm.ConnPool]*gorm.PreparedStmtDB{}
	}

	if dr.resolvers == nil {
		dr.resolvers = map[string]*resolver{}
	}

	if config.Policy == nil {
		config.Policy = RandomPolicy{}
	}

	config.datas = datas
	dr.configs = append(dr.configs, config)
	if dr.DB != nil {
		dr.compileConfig(config)
	}
	return dr
}

func (dr *DBResolver) Name() string {
	return "gorm:db_resolver"
}

func (dr *DBResolver) Initialize(db *gorm.DB) error {
	dr.DB = db
	dr.registerCallbacks(db)
	return dr.compile()
}

func (dr *DBResolver) compile() error {
	for _, config := range dr.configs {
		if err := dr.compileConfig(config); err != nil {
			return err
		}
	}
	return nil
}

func (dr *DBResolver) compileConfig(config Config) (err error) {
	var (
		connPool = dr.DB.Config.ConnPool
		r        = resolver{
			dbResolver:        dr,
			policy:            config.Policy,
			traceResolverMode: config.TraceResolverMode,
		}
	)

	if preparedStmtDB, ok := connPool.(*gorm.PreparedStmtDB); ok {
		connPool = preparedStmtDB.ConnPool
	}

	if len(config.Sources) == 0 {
		r.sources = []NamedConnPool{
			{
				Name:     "Default",
				ConnPool: connPool,
			},
		}
	} else if r.sources, err = dr.convertToConnPool(config.Sources); err != nil {
		return err
	}

	if len(config.Replicas) == 0 {
		r.replicas = r.sources
	} else if r.replicas, err = dr.convertToConnPool(config.Replicas); err != nil {
		return err
	}

	if len(config.datas) > 0 {
		for _, data := range config.datas {
			if t, ok := data.(string); ok {
				dr.resolvers[t] = &r
			} else {
				stmt := &gorm.Statement{DB: dr.DB}
				if err := stmt.Parse(data); err == nil {
					dr.resolvers[stmt.Table] = &r
				} else {
					return err
				}
			}
		}
	} else if dr.global == nil {
		dr.global = &r
	} else {
		return errors.New("conflicted global resolver")
	}

	for _, fc := range dr.compileCallbacks {
		if err = r.call(fc); err != nil {
			return err
		}
	}

	if config.TraceResolverMode {
		dr.Logger = NewResolverModeLogger(dr.Logger)
	}

	return nil
}

func (dr *DBResolver) convertToConnPool(dialectors NamedDialectors) (connPools []NamedConnPool, err error) {
	config := *dr.DB.Config
	for _, d := range dialectors {
		if db, err := gorm.Open(d.Dialector, &config); err == nil {
			connPool := db.Config.ConnPool
			if preparedStmtDB, ok := connPool.(*gorm.PreparedStmtDB); ok {
				connPool = preparedStmtDB.ConnPool
			}

			dr.prepareStmtStore[connPool] = &gorm.PreparedStmtDB{
				ConnPool:    db.Config.ConnPool,
				Stmts:       map[string]*gorm.Stmt{},
				Mux:         &sync.RWMutex{},
				PreparedSQL: make([]string, 0, 100),
			}

			connPools = append(connPools, NamedConnPool{
				ConnPool: connPool,
				Name:     d.Name,
			})
		} else {
			return nil, err
		}
	}

	return connPools, err
}

func (dr *DBResolver) resolve(stmt *gorm.Statement, op Operation) gorm.ConnPool {
	if len(dr.resolvers) > 0 {
		if u, ok := stmt.Clauses[usingName].Expression.(using); ok && u.Use != "" {
			if r, ok := dr.resolvers[u.Use]; ok {
				return r.resolve(stmt, op)
			}
		}

		if stmt.Table != "" {
			if r, ok := dr.resolvers[stmt.Table]; ok {
				return r.resolve(stmt, op)
			}
		}

		if stmt.Schema != nil {
			if r, ok := dr.resolvers[stmt.Schema.Table]; ok {
				return r.resolve(stmt, op)
			}
		}

		if rawSQL := stmt.SQL.String(); rawSQL != "" {
			if r, ok := dr.resolvers[getTableFromRawSQL(rawSQL)]; ok {
				return r.resolve(stmt, op)
			}
		}
	}

	if dr.global != nil {
		return dr.global.resolve(stmt, op)
	}

	return stmt.ConnPool
}

type ResolverConnStats struct {
	Stats sql.DBStats `json:",inline"`
	Name  string      `json:"name"`
}

type ResolverConnPoolStats struct {
	Connections int                 `json:"connections"`
	Source      []ResolverConnStats `json:"source"`
	Replicas    []ResolverConnStats `json:"replicas"`
}

func (dr *DBResolver) GetConnPoolStats() ResolverConnPoolStats {
	sourceStats := make([]ResolverConnStats, 0)
	replicaStats := make([]ResolverConnStats, 0)
	var connections int
	globalResolver := dr.global
	for _, s := range globalResolver.sources {
		if db, ok := s.ConnPool.(*sql.DB); ok {
			sourceStats = append(sourceStats, ResolverConnStats{
				Name:  s.Name,
				Stats: db.Stats(),
			})
			connections++
		}
	}
	for _, r := range globalResolver.replicas {
		if db, ok := r.ConnPool.(*sql.DB); ok {
			replicaStats = append(replicaStats, ResolverConnStats{
				Name:  r.Name,
				Stats: db.Stats(),
			})
			connections++
		}
	}
	return ResolverConnPoolStats{
		Connections: connections,
		Source:      sourceStats,
		Replicas:    replicaStats,
	}
}

func hydrateConnections(db *sql.DB, conns, workers int) error {
	var wg sync.WaitGroup
	work := make(chan bool)
	connChan := make(chan *sql.Conn, conns)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range work {
				ctx, _ := context.WithTimeout(context.Background(), time.Second*5)
				conn, err := db.Conn(ctx)
				if err != nil {
					log.Printf("error opening hydration conn: %s", err)
					connChan <- nil
					return
				}
				connChan <- conn
			}
		}()
	}
	for i := 0; i < conns; i++ {
		work <- true
	}
	close(work)
	wg.Wait()
	close(connChan)
	for conn := range connChan {
		if conn == nil {
			continue
		}
		err := conn.Close()
		if err != nil {
			log.Printf("error closing hydration conn %s", err)
		}
	}
	return nil
}

func (dr *DBResolver) HydrateConnections(conns, workers int) error {
	globalResolver := dr.global
	for _, s := range globalResolver.sources {
		if db, ok := s.ConnPool.(*sql.DB); ok {
			diff := conns - db.Stats().OpenConnections
			log.Printf("diff source: %d", diff)
			if diff > 0 {
				hydrateConnections(db, diff+1, workers)
			}
		}
	}
	for _, r := range globalResolver.replicas {
		if db, ok := r.ConnPool.(*sql.DB); ok {
			diff := conns - db.Stats().OpenConnections
			log.Printf("diff replica: %d", diff)
			if diff > 0 {
				hydrateConnections(db, diff+1, workers)
			}
		}
	}
	return nil
}
