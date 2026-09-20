// gscsvc 地面站窗口冲突处置服务入口。
package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"gscsvc/internal/api"
	"gscsvc/internal/service"
	"gscsvc/internal/store"
)

func main() {
	dbPath := flag.String("db", "gscsvc.db", "SQLite 数据库文件路径")
	addr := flag.String("addr", ":8080", "HTTP 监听地址")
	flag.Parse()

	st, err := store.Open(*dbPath)
	if err != nil {
		log.Fatalf("打开数据库失败: %v", err)
	}
	defer st.Close()

	svc := service.New(st, nil)
	rep, err := svc.StartupRecovery(context.Background())
	if err != nil {
		log.Fatalf("启动恢复失败: %v", err)
	}
	log.Printf("启动恢复完成：待决人工决策 %d 项", rep.PendingDecisions)

	r := api.NewRouter(svc)
	srv := &http.Server{Addr: *addr, Handler: r}

	go func() {
		log.Printf("gscsvc 监听 %s（数据库 %s）", *addr, *dbPath)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP 服务异常: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Printf("收到退出信号，正在优雅关闭…")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Fatalf("关闭失败: %v", err)
	}
	log.Printf("已退出")
}
