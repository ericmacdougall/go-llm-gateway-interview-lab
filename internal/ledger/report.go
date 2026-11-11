package ledger

import (
	"context"
	"errors"
)

type UsageGroup struct {
	Workload   string `json:"workload"`
	Department string `json:"department"`
	App        string `json:"app"`
	Model      string `json:"model"`
	State      string `json:"state"`
	Attempts   int64  `json:"attempts"`
	Confirmed  int64  `json:"confirmed_units"`
	Held       int64  `json:"held_units"`
}

// Report aggregates the persisted scope snapshots for one explicit client.
func (s *Store) Report(ctx context.Context, client string) ([]UsageGroup, error) {
	if client == "" {
		return nil, errors.New("explicit client required")
	}
	rows, err := s.DB.QueryContext(ctx, `
		SELECT attempt->'principal'->>'workload',
			COALESCE(attempt->'principal'->>'department',''),
			COALESCE(attempt->'principal'->>'app',''),attempt->'route'->>'model',state,
			count(*),COALESCE(sum(actual),0),
			COALESCE(sum(CASE WHEN state IN ('reserved','dispatching','uncertain') THEN estimate ELSE 0 END),0)
		FROM gateway_attempts WHERE client_id=$1
		GROUP BY 1,2,3,4,5 ORDER BY 1,2,3,4,5`, client)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	groups := make([]UsageGroup, 0)
	for rows.Next() {
		var group UsageGroup
		if err := rows.Scan(&group.Workload, &group.Department, &group.App, &group.Model, &group.State, &group.Attempts, &group.Confirmed, &group.Held); err != nil {
			return nil, err
		}
		groups = append(groups, group)
	}
	return groups, rows.Err()
}
