package hmongo

// 分页信息
type Page struct {
	PageNo     int64 `json:"page_no"`
	PageSize   int64 `json:"page_size"`
	TotalPage  int64 `json:"total_page"`
	TotalCount int64 `json:"total_count"`
	FirstPage  bool  `json:"first_page"`
	LastPage   bool  `json:"last_page"`
}

// PageUtil生成分页结构工具函数
func PageUtil(count int64, pageNo int64, pageSize int64) *Page {
	if pageSize == 0 {
		pageSize = 5
	}
	tp := count / pageSize
	if count%pageSize > 0 || count == 0 {
		tp = count/pageSize + 1
	}
	return &Page{PageNo: pageNo, PageSize: pageSize, TotalPage: tp, TotalCount: count, FirstPage: pageNo == 1, LastPage: pageNo == tp}
}
