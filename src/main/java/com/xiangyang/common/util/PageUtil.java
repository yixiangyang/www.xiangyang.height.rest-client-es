package com.xiangyang.common.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * 分页工具
 *
 */
public final class PageUtil {
	private static final Logger LOGGER = LoggerFactory.getLogger(PageUtil.class);
	
	private PageUtil() {
	}
	
	/**
	 * 获取数据偏移量
	 * @param pageSize
	 * @param nextPage
	 * @param totalCount
	 * @return -1 表示偏移量过大
	 */
	public static int getOffset(int pageSize, int nextPage, int totalCount) {
		int offset = (nextPage-1)*pageSize;
		return totalCount > offset? offset : -1;
	}
	
	/**
	 * 获取总页数
	 * @param totalCount
	 * @param pageSize
	 * @return
	 * @throws TOTAL_PAGE_INVALID / PAGE_SIZE_INVALID
	 */
	public static int getTotalPage(int totalCount, int pageSize) {
		if (totalCount <= 0) {
			LOGGER.error("总记录数不能为0或负数, totalCount={}", totalCount);
		}
		if (pageSize <= 0) {
			LOGGER.error("单页数量不能为0或负数, pageSize={}", pageSize);
		}
		int mod = totalCount % pageSize;
		if (mod == 0) {
			return totalCount / pageSize;
		}
		return totalCount / pageSize + 1;
	}
	
}
