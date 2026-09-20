// Command gwsvc 启动地面站窗口冲突处置服务。
package main

import (
	"flag"
	"log"

	"satops/groundstation/internal/api"
	"satops/groundstation/internal/service"
	"satops/groundstation/internal/store"
)

func main() {
	addr := flag.String("addr", ":8080", "HTTP 监听地址")
	dbPath := flag.String("db", "gwsvc.db", "SQLite 数据库文件路径（:memory: 为内存库）")
	flag.Parse()

	st, err := store.Open(*dbPath)
	if err != nil {
		log.Fatalf("打开数据库失败: %v", err)
	}
	defer st.Close()

	svc := service.New(st)
	srv := api.NewServer(svc)

	log.Printf("地面站窗口冲突处置服务启动，监听 %s，数据库 %s", *addr, *dbPath)
	if err := srv.Engine().Run(*addr); err != nil {
		log.Fatalf("服务退出: %v", err)
	}
}
