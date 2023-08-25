package com.cicc;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import cn.hutool.core.util.StrUtil;
import com.cicc.comm.CommStartup;
import com.cicc.comm.action.ActionService;
import com.cicc.comm.config.ConfigManager;
import com.cicc.comm.queue.QueueService;
import com.cicc.db.BeetlSqlUtils;
import com.cicc.db.DBManager;
import com.cicc.db.event.DBEvent2OracleHandler;
import com.cicc.db.event.DBEventQueueService;
import com.cicc.db.procedure.CallPosLoadProcedure;
import com.cicc.flash.cache.*;
import com.cicc.flash.exception.BLException;
import com.cicc.flash.credit.CreditCalcEntrance;
import com.cicc.flash.exception.ErrorMessage;
import com.cicc.flash.global.Constants;
import com.cicc.flash.global.ContractNumManager;
import com.cicc.flash.global.ExchErrorListManager;
import com.cicc.flash.global.SerialNumManager;
import com.cicc.flash.loader.AccountLoader;
import com.cicc.flash.loader.PlatFormInfoLoader;
import com.cicc.flash.loader.PositionLoader;
import com.cicc.flash.loader.credit.*;
import com.cicc.flash.recover.RecoverService;
import com.cicc.flash.service.PushPersistenceService;
import com.cicc.flash.service.PushResponseService;
import com.cicc.offer.OfferService;

import cn.hutool.core.thread.ThreadUtil;
import cn.hutool.cron.CronUtil;
import cn.hutool.setting.Setting;
import com.cicc.quotation.AmaQuotation;
import com.cicc.quotation.PlatformStateInfoManager;
import com.cicc.utils.DateUtil;
import sun.awt.windows.ThemeReader;

/**
 * 启动入口
 *
 * /// 配置文件使用示例DisruptorConfig config = SystemConfig.disruptor;
 *
 * @author guanzl
 * @time 2019年3月12日 下午5:50:25
 */
public class App {

    public static void main(String[] args) {
        long t1 = System.currentTimeMillis();
        try {
            final CountDownLatch CDL = new CountDownLatch(22);
            final CountDownLatch CDL2 = new CountDownLatch(13);
            DateUtil.startFormatTime();
            // 1.加载配置文件
            ConfigManager.load(args[0]);
            Setting setting = ConfigManager.getSetting();
            // 2.启动BeetlSql，连接oracle
            BeetlSqlUtils.getInstance();
            // 3.从数据库读取数据
            ThreadUtil.execute(() -> {StockCache.getInstance();					CDL.countDown();});
            ThreadUtil.execute(() -> {TradingFeeCache.getInstance();			CDL.countDown();});
            ThreadUtil.execute(() -> {CommCache.getInstance();					CDL.countDown();});
            ThreadUtil.execute(() -> {ErrorMessage.getInstance();				CDL.countDown();});
            ThreadUtil.execute(() -> {ExchErrorListManager.getInstance();		CDL.countDown();});
            ThreadUtil.execute(() -> {OrderApplCache.getInstance();				CDL.countDown();});
            ThreadUtil.execute(() -> {IpoRightsCache.getInstance();				CDL.countDown();});
            ThreadUtil.execute(() -> {EtfCache.getInstance();					CDL.countDown();});
            ThreadUtil.execute(() -> {FixedPriceParaCache.getInstance();		CDL.countDown();});
            ThreadUtil.execute(() -> {RepurchaseFeeCache.getInstance();			CDL.countDown();});
            ThreadUtil.execute(() -> {ContractNumManager.getInstance();			CDL.countDown();});
            ThreadUtil.execute(() -> {SerialNumManager.getInstance();			CDL.countDown();});
            ThreadUtil.execute(() -> {RestrictedCustomerCache.getInstance();			CDL.countDown();});

            ThreadUtil.execute(() -> {
                PlatformStateInfoManager.getInstance();	CDL.countDown();});
            //双融风控增加
            ThreadUtil.execute(() -> {
                CreditAccountCache.getInstance().refresh();					CDL.countDown();});
            ThreadUtil.execute(() -> {
                GurantyCache.getInstance();					CDL.countDown();});
            ThreadUtil.execute(() -> {
                CsConsentrateCache.getInstance();			CDL.countDown();});
            ThreadUtil.execute(() -> {
                HolidayCache.getInstance();			CDL.countDown();});
            ThreadUtil.execute(() -> {
                StkCMORateCache.getInstance();			CDL.countDown();});
            ThreadUtil.execute(() -> {
                StkConsentrateCache.getInstance();			CDL.countDown();});
            ThreadUtil.execute(() -> {
                AccountMarginCache.getInstance();			CDL.countDown();});
            ThreadUtil.execute(() -> {GlobalConstCache.getInstance();			CDL.countDown();});
            CDL.await();
            //调用oms-flash持仓切换存储过程
            ThreadUtil.execute(() -> {new CallPosLoadProcedure().execute(); 	CDL2.countDown();});
            // 加载actionService
            ThreadUtil.execute(() -> {ActionService.loadService("com.cicc.flash.service.impl");CDL2.countDown();});
            ThreadUtil.execute(() -> {FTUserCache.getInstance();				CDL2.countDown();});
            ThreadUtil.execute(() -> {GlobalParaCache.getInstance();			CDL2.countDown();});
            ThreadUtil.execute(() -> {BranchInfoCache.getInstance();			CDL2.countDown();});
            ThreadUtil.execute(() -> {DeskInfoCache.getInstance();				CDL2.countDown();});
            ThreadUtil.execute(() -> {AlgoFeeConfigCache.getInstance();			CDL2.countDown();});
            // 启动extremeDB
            ThreadUtil.execute(() -> {
                DBEventQueueService.startUp(new DBEvent2OracleHandler());
                DBManager.getInstance();
                CDL2.countDown();
            });
            ThreadUtil.execute(() -> {QueueService.startUp();					CDL2.countDown();});
            ThreadUtil.execute(() -> {InvestorCache.getInstance();					CDL2.countDown();});
            ThreadUtil.execute(() -> {SysConfigCache.getInstance();             CDL2.countDown();});
            ThreadUtil.execute(() -> {CreditConcentrateRateSettingCache.getInstance();         CDL2.countDown();});
            // 启动推送消息持久化
            ThreadUtil.execute(() -> {PushPersistenceService.startUp();	        CDL2.countDown();});
            // 等待所有子线程的业务都处理完成（计数器的count==0时）
            CDL2.await();

            if (setting.getBool("recover")) {
                if (setting.getBool("credit")) {
                    new RecoverService().recoverAll();
                } else {
                    new RecoverService().recover();
                }
            }

            // Initialize and run logging
            DBManager.getInstance().initRunLog();

            if (!setting.getBool("recover")) {
                final CountDownLatch CDL3 = new CountDownLatch(9);
                ThreadUtil.execute(() -> {
                    new PositionLoader().load();
                    CDL3.countDown();
                });
                ThreadUtil.execute(() -> {
                    new PlatFormInfoLoader().load();
                    CDL3.countDown();
                });
                ThreadUtil.execute(() -> {
                    new AccountLoader().load();
                    CDL3.countDown();
                });
                ThreadUtil.execute(() -> {
                    new SBLStockLoader().load();
                    CDL3.countDown();
                });
                ThreadUtil.execute(() -> {
                    new CreditShareAmtUseDetailLoader().load();
                    CDL3.countDown();
                });
                ThreadUtil.execute(() -> {
                    new AccountMarginLoader().load();
                    CDL3.countDown();
                });
                ThreadUtil.execute(() -> {
                    new CreditCashLogLoader().load();
                    CDL3.countDown();
                });
                ThreadUtil.execute(() -> {
                    new CreditShareLogLoader().load();
                    CDL3.countDown();
                });
                ThreadUtil.execute(() -> {new CreditCashReturnDailyLoader().load();			CDL3.countDown();});
                CDL3.await();
            }
            // 启动推送服务
            PushResponseService.startUp();
            //开启disruptor
            CreditCalcEntrance.getInstance().start();

            CreditAccountCache.getInstance().initCreditInfo();
            // 预热
            warmup();

            while(true) {
                ConcurrentHashMap<String, AtomicInteger> ordercalcMap =  CreditAccountCache.getInstance().orderCalcCountMap;
                int sumcals = 0;
                for(AtomicInteger ai:ordercalcMap.values()) {
                    sumcals += ai.get();
                }
                if (sumcals == 0)
                    break;
                System.out.println("等待一秒后继续判断是否开启报盘和通讯服务");
                Thread.sleep(1000);
            }
            // 5.启动报盘服务
            OfferService.startUp();
            // 6.启动服务器
            CommStartup.init();

            //开启行情服务
            if (setting.getBool("loadQuotation")) {
                AmaQuotation.getInstance().start();
                AmaQuotation.getInstance().addsubscribe(CreditAccountCache.getInstance().getQuotSet());
            }

            // 9.计划任务
            if (setting.getBool("loadCron")) {
                CronUtil.setMatchSecond(true);// 支持秒级别定时任务
                CronUtil.start();
            }

            //系统启动完成后主动GC一次
            System.gc();
            System.out.println("划转截止时间："+ StrUtil.emptyToDefault(ConfigManager.getSetting().getStr("transferTimeLimit"), Constants.DEFAULT_TRANSFER_TIME_LIMIT));
            System.out.println("系统启动耗时："+ (System.currentTimeMillis()-t1)+"ms");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    private static void warmup() {
        try {
            if (HolidayCache.getInstance().isHoliday()) {
                return;
            }
            //获取合同号
            ContractNumManager.getInstance().getFullContractNum("0", "43979");
            ContractNumManager.getInstance().getFullContractNum("1", "398294");
        } catch (BLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }
}
