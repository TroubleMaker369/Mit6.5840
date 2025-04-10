package kvraft

import (
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var SugarKVLogger *zap.SugaredLogger

func InitKVLogger() {
	writeSyncer := getLogWriter()
	encoder := getEncoder()
	core := zapcore.NewCore(encoder, writeSyncer, zapcore.DebugLevel)

	logger := zap.New(core)
	SugarKVLogger = logger.Sugar()
}

func getEncoder() zapcore.Encoder {
	return zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig())
}

func getLogWriter() zapcore.WriteSyncer {
	//如果想要追加写入可以查看我的博客文件操作那一章
	file, _ := os.OpenFile("/home/xy/mit2024/6.5840/src/kvraft/test.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	return zapcore.AddSync(file)
}
