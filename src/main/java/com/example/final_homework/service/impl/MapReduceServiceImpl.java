package com.example.final_homework.service.impl;

import com.example.final_homework.config.ReduceJobsConfiguration;
import com.example.final_homework.service.IHdfsService;
import com.example.final_homework.service.MapReduceService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

/**
 * @author MrCh1ppy
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class MapReduceServiceImpl implements MapReduceService {
	private final IHdfsService hdfsService;

	private final ReduceJobsConfiguration reduceJobsConfiguration;

	@Override
	public void wordCount(String jobName, String inputPath, String outputPath) throws Exception {
		if (!StringUtils.hasText(jobName) || !StringUtils.hasText(inputPath)) {
			return;
		}
		// 输出目录 = output/当前Job,如果输出路径存在则删除，保证每次都是最新的
		if (hdfsService.existFile(outputPath)) {
			hdfsService.deleteFile(outputPath);
		}
		log.info("check");
		reduceJobsConfiguration.getWordCountJobsConf(jobName, inputPath, outputPath);
	}

}
