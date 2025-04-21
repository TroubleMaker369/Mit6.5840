package shardkv

import (
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var ShardkvLogger *zap.SugaredLogger

func InitLogger() {
	writeSyncer := getLogWriter()
	encoder := getEncoder()
	core := zapcore.NewCore(encoder, writeSyncer, zapcore.DebugLevel)

	logger := zap.New(core)
	ShardkvLogger = logger.Sugar()
}

func getEncoder() zapcore.Encoder {
	return zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig())
}

func getLogWriter() zapcore.WriteSyncer {
	//如果想要追加写入可以查看我的博客文件操作那一章
	file, _ := os.OpenFile("/home/xy/mit2024/6.5840/src/shardkv/test.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	return zapcore.AddSync(file)
}

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		ShardkvLogger.Infof(format, a...)
		// log.Printf(format, a...)
	}
}
